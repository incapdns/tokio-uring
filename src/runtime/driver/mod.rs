use crate::buf::fixed::FixedBuffers;
use crate::runtime::driver::op::{ArcMonitor, Completable, CqeResult, Lifecycle, Location, MultiCQE, Notifiable, OneshotCQE, Op, Updateable};
pub(crate) use handle::*;
use io_uring::{cqueue, opcode, squeue, CompletionQueue, IoUring, SubmissionQueue};
use std::collections::LinkedList;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

mod handle;
pub(crate) mod op;

pub(crate) struct Driver {
  /// IoUring bindings
  uring: IoUring,

  /// Accessing completion_shared
  accessing_completion_shared: AtomicBool,

  /// Accessing completion_shared
  accessing_submission_shared: AtomicBool,

  /// Number of pending operations
  pending_number: AtomicUsize,

  /// Reference to the currently registered buffers.
  /// Ensures that the buffers are not dropped until
  /// after the io-uring runtime has terminated.
  fixed_buffers: Option<Rc<dyn FixedBuffers>>,
}

impl Driver {
  pub(crate) fn new(b: &crate::Builder) -> io::Result<Driver> {
    let uring = b.urb.build(b.entries)?;

    Ok(Driver {
      uring,
      accessing_completion_shared: AtomicBool::new(false),
      accessing_submission_shared: AtomicBool::new(false),
      pending_number: AtomicUsize::new(0),
      fixed_buffers: None,
    })
  }

  fn wait(&self) -> io::Result<usize> {
    self.uring.submit_and_wait(1)
  }

  // only used in tests rn
  #[allow(unused)]
  pub(super) fn num_operations(&self) -> usize {
    self.pending_number.load(Ordering::Acquire)
  }

  pub(crate) fn submit(&self, sq: &mut SubmissionQueue) -> io::Result<()> {
    loop {
      match self.uring.submit() {
        Ok(_) => {
          sq.sync();
          return Ok(());
        }
        Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {
          self.dispatch_completions();
        }
        Err(e) if e.raw_os_error() != Some(libc::EINTR) => {
          return Err(e);
        }
        _ => continue,
      }
    }
  }

  fn try_submit(&self, sqe: &mut squeue::Entry) -> io::Result<()> {
    let mut res: io::Result<()> = Ok(());

    self.access_submission_shared(|mut sq| unsafe {
      while sq.push(sqe).is_err() {
        res = self.submit(&mut sq);
        if res.is_err() {
          break;
        }
      }
    });

    res
  }

  fn force_submit(&self) {
    self.access_submission_shared(|mut sq| {
      while !sq.is_empty() {
        let res = self.submit(&mut sq);
        res.expect("Internal error when dropping driver");
      }
    });
  }

  #[inline(always)]
  fn try_access_completion_shared(&self, callback: impl FnOnce(CompletionQueue)){
    if self.accessing_completion_shared.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
      let cq = unsafe { self.uring.completion_shared() };
      callback(cq);
      self.accessing_completion_shared.store(false, Ordering::Relaxed);
    }
  }

  #[inline(always)]
  fn access_submission_shared(&self, callback: impl FnOnce(SubmissionQueue)) {
    while self.accessing_submission_shared.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {}
    let sq = unsafe { self.uring.submission_shared() };
    callback(sq);
    self.accessing_submission_shared.store(false, Ordering::Relaxed);
  }

  pub(crate) fn dispatch_completions(&self) {
    self.try_access_completion_shared(|mut cq|{
      cq.sync();

      for cqe in cq {
        if cqe.user_data() == u64::MAX {
          // Result of the cancellation action. There isn't anything we
          // need to do here. We must wait for the CQE for the operation
          // that was canceled.
          continue;
        }

        // SAFETY: The user_data field of the CQE is a pointer to Location struct
        // which Future is pinned in memory.
        let location: &mut ArcMonitor<Location> = unsafe { mem::transmute(cqe.user_data()) };
        let notifiable: &mut dyn Notifiable =
          unsafe { mem::transmute((location.address, location.vtable)) };

        let mut is_last = false;

        // Only set last if there are no more CQE's and
        // the location is not used.
        if !cqueue::more(cqe.flags()) {
          is_last = true;
        }

        // Only notify if the result is not ECANCELED
        // and Op is not dropped.
        if cqe.result() != -libc::ECANCELED
        {                       // This will delay the drop of the Op until leave call
          if let Some(_guard) = location.try_enter() {
            notifiable.notify(cqe);
          }
        }

        if is_last {
          location.quit();
          self.pending_number.fetch_sub(1, Ordering::Release);
        }
      }
    });
  }

  pub(crate) fn register_buffers(
    &mut self,
    buffers: Rc<dyn FixedBuffers>,
  ) -> io::Result<()> {
    unsafe {
      self
        .uring
        .submitter()
        .register_buffers(buffers.as_ref().iovecs())
    }?;

    self.fixed_buffers = Some(buffers);
    Ok(())
  }

  pub(crate) fn unregister_buffers(
    &mut self,
    buffers: Rc<dyn FixedBuffers>,
  ) -> io::Result<()> {
    if let Some(currently_registered) = &self.fixed_buffers {
      if Rc::ptr_eq(&buffers, currently_registered) {
        self.uring.submitter().unregister_buffers()?;
        self.fixed_buffers = None;
        return Ok(());
      }
    }
    Err(io::Error::new(
      io::ErrorKind::Other,
      "fixed buffers are not currently registered",
    ))
  }

  fn set_ignored<T: Unpin, CqeType: Unpin>(op: &mut Op<T, CqeType>) {
    let data = op.take_data();
    let lifecycle = op.get_lifecycle();
    *lifecycle = Lifecycle::Ignored(Box::new(data));
  }

  pub(crate) fn remove_op<T: Unpin, CqeType: Unpin>(&self, op: &mut Op<T, CqeType>) {
    // Get the Op Lifecycle state from the driver
    let lifecycle = op.get_lifecycle();

    match mem::replace(lifecycle, Lifecycle::Submitted) {
      Lifecycle::Submitted | Lifecycle::Waiting(_) => {
        Driver::set_ignored(op);
      }
      Lifecycle::CompletionList(list) => {
        // Deallocate list entries, recording if more CQE's are expected
        let more = cqueue::more(list.into_iter().last().unwrap().flags);

        if more {
          // If more are expected, we have to keep the op around
          Driver::set_ignored(op);
        }
      }
      Lifecycle::Ignored(..) => unreachable!(),
      _ => {}
    }

    let location: &mut ArcMonitor<Location> = unsafe { mem::transmute(op.location()) };

    location.try_recycle(true);
  }

  pub(crate) fn submit_op_2<T: Unpin, S: Unpin>(
    &self,
    mut sqe: squeue::Entry,
    data: T,
    handle: WeakHandle,
  ) -> io::Result<Pin<Box<Op<T, S>>>> {
    // Create the operation
    let op = Op::new(handle, data);

    // Set the location (ptr, vtable) of the operation
    sqe = sqe.user_data(op.location());

    self.try_submit(&mut sqe)?;

    self.pending_number.fetch_add(1, Ordering::Release);

    Ok(op)
  }

  pub(crate) fn submit_op<T: Unpin, S: Unpin, F>(
    &self,
    mut data: T,
    f: F,
    handle: WeakHandle,
  ) -> io::Result<Pin<Box<Op<T, S>>>>
  where
    T: Completable,
    F: FnOnce(&mut T) -> squeue::Entry,
  {
    // Configure the SQE
    let mut sqe = f(&mut data);

    // Create the operation
    let op = Op::new(handle, data);

    // Set the location (ptr, vtable) of the operation
    sqe = sqe.user_data(op.location());

    self.try_submit(&mut sqe)?;

    self.pending_number.fetch_add(1, Ordering::Release);

    Ok(op)
  }

  pub(crate) fn poll_op_oneshot<T>(
    &self,
    op: &mut Op<T, OneshotCQE>,
    cx: &mut Context<'_>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable,
  {
    let lifecycle = op.get_lifecycle();

    match mem::replace(lifecycle, Lifecycle::Submitted) {
      Lifecycle::Initial | Lifecycle::Submitted => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) if !waker.will_wake(cx.waker()) => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) => {
        *lifecycle = Lifecycle::Waiting(waker);
        Poll::Pending
      }
      Lifecycle::Ignored(..) => unreachable!(),
      Lifecycle::Completed(cqe) => Poll::Ready(op.take_data().unwrap().complete(cqe.into())),
      Lifecycle::CompletionList(..) => {
        unreachable!("No `more` flag set for OneshotCQE")
      }
    }
  }

  pub(crate) fn poll_op<T>(&self, op: &mut Op<T>, cx: &mut Context<'_>) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable,
  {
    let lifecycle = op.get_lifecycle();

    match mem::replace(lifecycle, Lifecycle::Submitted) {
      Lifecycle::Submitted | Lifecycle::Initial => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) if !waker.will_wake(cx.waker()) => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) => {
        *lifecycle = Lifecycle::Waiting(waker);
        Poll::Pending
      }
      Lifecycle::Ignored(..) => unreachable!(),
      Lifecycle::Completed(cqe) => Poll::Ready(op.take_data().unwrap().complete(cqe.into())),
      Lifecycle::CompletionList(..) => {
        unreachable!("No `more` flag set for SingleCQE")
      }
    }
  }

  fn process_completion_list<T>(
    &self,
    cx: &mut Context<'_>,
    op: &mut Op<T, MultiCQE>,
    list: LinkedList<CqeResult>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable + Updateable,
  {
    let mut data = op.take_data().unwrap();
    let mut status = Poll::Pending;
    // Consume the CqeResult list, calling update on the Op on all Cqe's flagged `more`
    // If the final Cqe is present, clean up and return Poll::Ready
    for cqe in list {
      if cqueue::more(cqe.flags) {
        data.update(cqe);
      } else {
        status = Poll::Ready(cqe);
        break;
      }
    }
    match status {
      Poll::Pending => {
        // We need more CQE's. Restore the op state
        op.insert_data(data);
        *op.get_lifecycle() = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Poll::Ready(cqe) => Poll::Ready(data.complete(cqe)),
    }
  }

  pub(crate) fn poll_multishot_op<T>(
    &self,
    op: &mut Op<T, MultiCQE>,
    cx: &mut Context<'_>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable + Updateable,
  {
    let lifecycle = op.get_lifecycle();

    match mem::replace(lifecycle, Lifecycle::Submitted) {
      Lifecycle::Initial | Lifecycle::Submitted => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) if !waker.will_wake(cx.waker()) => {
        *lifecycle = Lifecycle::Waiting(cx.waker().clone());
        Poll::Pending
      }
      Lifecycle::Waiting(waker) => {
        *lifecycle = Lifecycle::Waiting(waker);
        Poll::Pending
      }
      Lifecycle::Ignored(..) => unreachable!(),
      Lifecycle::Completed(cqe) => {
        // This is possible. We may have previously polled a CompletionList,
        // and the final CQE registered as Completed
        Poll::Ready(op.take_data().unwrap().complete(cqe.into()))
      }
      Lifecycle::CompletionList(list) => self.process_completion_list(cx, op, list),
    }
  }
}

impl AsRawFd for Driver {
  fn as_raw_fd(&self) -> RawFd {
    self.uring.as_raw_fd()
  }
}

/// Drop the driver, cancelling any in-progress ops and waiting for them to terminate.
///
/// This first cancels all ops and then waits for them to be moved to the completed lifecycle phase.
///
/// It is possible for this to be run without previously dropping the runtime, but this should only
/// be possible in the case of [`std::process::exit`].
///
/// This depends on us knowing when ops are completed and done firing.
/// When multishot ops are added (support exists but none are implemented), a way to know if such
/// an op is finished MUST be added, otherwise our shutdown process is unsound.
impl Drop for Driver {
  fn drop(&mut self) {
    // get all ops in flight for cancellation
    self.force_submit();

    let any_flags_number: u8 = 4;
    let any_flags: squeue::Flags = unsafe { mem::transmute(any_flags_number) };

    unsafe {
      let cancel = opcode::AsyncCancel::new(u64::MAX);
      let sqe = cancel.build().flags(any_flags);

      while self.uring.submission().push(&sqe).is_err() {
        self
          .uring
          .submit_and_wait(1)
          .expect("Internal error when dropping driver");
      }
    }

    while self.num_operations() > 0 {
      let _ = self.wait();
      self.dispatch_completions();
    }
  }
}

#[cfg(test)]
mod test {

  /// Asserts that at least duration has elapsed since the start instant ±1ms.
  ///
  /// ```rust
  /// use tokio::time::{self, Instant};
  /// use std::time::Duration;
  /// use tokio_test::assert_elapsed;
  /// # async fn test_time_passed() {
  ///
  /// let start = Instant::now();
  /// let dur = Duration::from_millis(50);
  /// time::sleep(dur).await;
  /// # }
  /// ```
  ///
  /// This 1ms buffer is required because Tokio's hashed-wheel timer has finite time resolution and
  /// will not always sleep for the exact interval.
  #[macro_export]
  macro_rules! assert_at_least_elapsed {
    ($start:expr, $dur:expr) => {{
        let elapsed = $start.elapsed();
        // type ascription improves compiler error when wrong type is passed
        let lower: std::time::Duration = $dur;

        // Handles ms rounding
        assert!(
            elapsed >= lower && lower <= elapsed + std::time::Duration::from_millis(1),
            "actual = {:?}, expected = {:?}",
            elapsed,
            lower
        );
    }};
  }

  use std::time::Duration;
  use crate::runtime::CONTEXT;

  fn num_operations() -> usize {
    CONTEXT.with(|cx| {
      let handle = cx.handle();
      handle.inner.num_operations()
    })
  }

  #[test]
  fn try_read_file() {
    crate::start(async {
      assert_eq!(num_operations(), 0);

      tokio::time::sleep(Duration::from_millis(1000)).await;

      let file = crate::fs::File::open("/etc/hosts").await.unwrap();

      let buf = vec![0; 4096];

      let (res, buf) = file.read_at(buf, 0).await;
      let n = res.unwrap();

      // Display the contents
      println!("{:?}", &buf[..n]);
    });
  }

  #[test]
  fn complete_after_drop() {
    crate::start(async {
      assert_eq!(num_operations(), 0);

      let start = std::time::Instant::now();

      let duration = Duration::from_secs(2);

      tokio::time::sleep(duration).await;

      // This ensures at least one sleep op is done
      assert_at_least_elapsed!(start, duration);

      assert_eq!(num_operations(), 0);
    });
  }
}