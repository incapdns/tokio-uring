use crate::runtime::driver::WeakHandle;
use crate::runtime::{driver, CONTEXT};
use atomic::Atomic;
use atomic_wait::{wait, wake_all};
use bytemuck::NoUninit;
use io_uring::{cqueue, squeue};
use std::cmp::PartialEq;
use std::collections::LinkedList;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::ptr::null;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};
use std::{io, mem};

/// An unsubmitted oneshot operation.
pub struct UnsubmittedOneshot<D: 'static, T: OneshotOutputTransform<StoredData = D>> {
  stable_data: D,
  post_op: T,
  sqe: squeue::Entry,
}

impl<D, T: OneshotOutputTransform<StoredData = D>> UnsubmittedOneshot<D, T>
where
  T::Output: Unpin + 'static,
  T: Unpin,
  D: Unpin,
{
  /// Construct a new operation for later submission.
  pub fn new(stable_data: D, post_op: T, sqe: squeue::Entry) -> Self {
    Self {
      stable_data,
      post_op,
      sqe,
    }
  }

  /// Submit an operation to the driver for batched entry to the kernel.
  pub fn submit(self) -> Pin<Box<Op<InFlightOneshot<D, T>, OneshotCQE>>> {
    CONTEXT.with(|x| self.submit_with_driver(&**x.handle()))
  }

  fn submit_with_driver(
    self,
    driver: &driver::Handle,
  ) -> Pin<Box<Op<InFlightOneshot<D, T>, OneshotCQE>>> {
    let inner = InFlightOneshotInner {
      stable_data: self.stable_data,
      post_op: self.post_op,
    };

    let data = InFlightOneshot { inner: Some(inner) };

    driver.submit_op_2(self.sqe, data).unwrap()
  }
}

/// An in-progress oneshot operation which can be polled for completion.
pub struct InFlightOneshot<D: 'static, T: OneshotOutputTransform<StoredData = D>> {
  inner: Option<InFlightOneshotInner<D, T>>,
}

struct InFlightOneshotInner<D, T: OneshotOutputTransform<StoredData = D>> {
  stable_data: D,
  post_op: T,
}

/// Transforms the output of a oneshot operation into a more user-friendly format.
pub trait OneshotOutputTransform {
  /// The final output after the transformation.
  type Output;
  /// The stored data within the op.
  type StoredData;
  /// Transform the stored data and the cqe into the final output.
  fn transform_oneshot_output(self, data: Self::StoredData, cqe: CqeResult) -> Self::Output;
}

#[derive(PartialEq, Copy, Clone)]
enum ArcMonitorStatus {
  Live,
  InUse,
  Dropped,
}

unsafe impl NoUninit for ArcMonitorStatus {}

pub struct ArcMonitor<T> {
  status: Atomic<ArcMonitorStatus>,
  total: AtomicU32,
  uses: AtomicU32,
  data: T,
}

pub struct ArcMonitorGuard<'a, T> {
  arc_monitor: &'a ArcMonitor<T>,
}

impl<T> Drop for ArcMonitorGuard<'_, T> {
  fn drop(&mut self) {
    self.arc_monitor.leave();
  }
}

impl<T> ArcMonitor<T> {
  #[allow(dead_code)]
  #[inline(always)]
  fn new(data: T, total: u32) -> &'static mut ArcMonitor<T> {
    Box::leak(Box::new(ArcMonitor {
      status: Atomic::new(ArcMonitorStatus::Live),
      data,
      total: AtomicU32::new(total),
      uses: AtomicU32::new(0),
    }))
  }

  #[allow(dead_code)]
  #[inline(always)]
  fn release(&mut self) {
    //SAFETY: Was previously a leaked Box<T>
    let _ = unsafe { Box::from_raw(self) };
  }

  #[allow(dead_code)]
  #[inline(always)]
  fn leave(&self) {
    let old_uses = self.uses.fetch_sub(1, Ordering::Release);
    wake_all(&self.uses);

    if old_uses == 1 {
      let _ = self.status.compare_exchange(
        ArcMonitorStatus::InUse,
        ArcMonitorStatus::Live,
        Ordering::Release,
        Ordering::Relaxed,
      );
    }
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn container_ptr(&self) -> *const ArcMonitor<T> {
    self
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn as_ptr(&self) -> *const T {
    &self.data
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn as_mut_ptr(&mut self) -> *mut T {
    &mut self.data
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn try_enter(&self) -> Option<ArcMonitorGuard<T>> {
    let old_uses = self.uses.fetch_add(1, Ordering::Release);

    if old_uses > 0 {
      return Some(ArcMonitorGuard { arc_monitor: self });
    }

    if self
      .status
      .compare_exchange(
        ArcMonitorStatus::Live,
        ArcMonitorStatus::InUse,
        Ordering::Relaxed,
        Ordering::Relaxed,
      )
      .is_err()
    {
      self.leave();
      return None;
    }

    Some(ArcMonitorGuard { arc_monitor: self })
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn quit(&mut self) -> bool {
    let old_total = self.total.fetch_sub(1, Ordering::Relaxed);

    if old_total == 1 {
      self.release();
      return true;
    }

    false
  }

  #[allow(dead_code)]
  #[inline(always)]
  pub fn try_recycle(&mut self, wail_until_drop: bool) -> bool {
    if self.quit() {
      return true;
    }

    let mut uses = self.uses.load(Ordering::Acquire);

    if wail_until_drop {
      while self.status.load(Ordering::Acquire) != ArcMonitorStatus::Dropped {
        while uses != 0 {
          wait(&self.uses, uses);
          uses = self.uses.load(Ordering::Acquire);
        }

        if self
          .status
          .compare_exchange(
            ArcMonitorStatus::Live,
            ArcMonitorStatus::Dropped,
            Ordering::Release,
            Ordering::Relaxed,
          )
          .is_ok()
        {
          return true;
        }
      }
    }

    self
      .status
      .compare_exchange(
        ArcMonitorStatus::Live,
        ArcMonitorStatus::Dropped,
        Ordering::Release,
        Ordering::Relaxed,
      )
      .is_ok()
  }
}

impl<T> Deref for ArcMonitor<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.data
  }
}

impl<T> DerefMut for ArcMonitor<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    //SAFETY: ArcMonitor is a leaked Box<T>
    unsafe { mem::transmute(&mut self.data) }
  }
}

#[derive(Default)]
pub(crate) struct Location {
  pub(crate) address: usize,
  pub(crate) vtable: usize,
}

/// In-flight operation
pub struct Op<T: 'static, CqeType = SingleCQE>
where
  T: Unpin,
  CqeType: Unpin,
{
  driver: WeakHandle,

  // Self element address and vtable
  location: *const ArcMonitor<Location>,

  // Per-operation data
  data: Option<T>,

  // Operation state
  lifecycle: Lifecycle,

  // CqeType marker
  _cqe_type: PhantomData<CqeType>,
}

/// Allow multi-thread for all operations
unsafe impl<T: Unpin, CqeType: Unpin> Send for Op<T, CqeType> {}
unsafe impl<T: Unpin, CqeType: Unpin> Sync for Op<T, CqeType> {}

/// A Marker for Ops which expect only a single completion event
pub struct SingleCQE;

/// A Marker for Operations will process multiple completion events,
/// which combined resolve to a single Future value
pub struct MultiCQE;

/// A Marker for oneshot operations
pub struct OneshotCQE;

pub trait Completable {
  type Output;
  /// `complete` will be called for cqe's do not have the `more` flag set
  fn complete(self, cqe: CqeResult) -> Self::Output;
}

pub(crate) trait Updateable: Completable {
  /// Update will be called for cqe's which have the `more` flag set.
  /// The Op should update any internal state as required.
  fn update(&mut self, cqe: CqeResult);
}

pub(crate) trait Notifiable {
  /// Notify will be called to notify the Lifecycle that a CQE has arrived.
  /// This will call complete on lifecycle
  fn notify(&mut self, cqe: cqueue::Entry);
}

impl<T: Unpin, CqeType: Unpin> Notifiable for Op<T, CqeType> {
  fn notify(&mut self, cqe: cqueue::Entry) {
    self.lifecycle.complete(cqe);
  }
}

#[allow(dead_code)]
pub(crate) enum Lifecycle {
  /// The operation has not yet been submitted to uring
  Initial,

  /// The operation has been submitted to uring and is currently in-flight
  Submitted,

  /// The submitter is waiting for the completion of the operation
  Waiting(Waker),

  /// The submitter no longer has interest in the operation result. The state
  /// must be passed to the driver and held until the operation completes.
  Ignored(Box<dyn std::any::Any>),

  /// The operation has completed with a single cqe result
  Completed(cqueue::Entry),

  /// One or more completion results have been recieved
  /// This holds the indices uniquely identifying the list within the slab
  CompletionList(LinkedList<CqeResult>),
}

/// A single CQE entry
pub struct CqeResult {
  pub(crate) result: io::Result<u32>,
  pub(crate) flags: u32,
}

impl From<cqueue::Entry> for CqeResult {
  fn from(cqe: cqueue::Entry) -> Self {
    let res = cqe.result();
    let flags = cqe.flags();
    let result = if res >= 0 {
      Ok(res as u32)
    } else {
      Err(io::Error::from_raw_os_error(-res))
    };
    CqeResult { result, flags }
  }
}

impl<T: Unpin, CqeType: Unpin> Op<T, CqeType> {
  /// Create a new operation
  pub(super) fn new(driver: WeakHandle, data: T) -> Pin<Box<Self>> {
    let pinned = Pin::new(Box::new(Op {
      driver,
      location: null(),
      lifecycle: Lifecycle::Initial,
      data: Some(data),
      _cqe_type: PhantomData,
    }));

    pinned.initialize()
  }

  fn initialize(mut self: Pin<Box<Self>>) -> Pin<Box<Self>> {
    let location = ArcMonitor::<Location>::new(Default::default(), 2);

    let this = &*self as *const Op<T, CqeType> as *mut Op<T, CqeType>;
    let notifiable: *mut dyn Notifiable = this as *mut _;

    //SAFETY: Converting current address and vtable to a tuple
    let (address, vtable): (usize, usize) = unsafe { mem::transmute(notifiable) };
    location.address = address;
    location.vtable = vtable;

    self.location = location.container_ptr();

    self
  }

  pub(super) fn location(&self) -> u64 {
    self.location as u64
  }

  pub(super) fn get_lifecycle(&mut self) -> &mut Lifecycle {
    &mut self.lifecycle
  }

  pub(super) fn take_data(&mut self) -> Option<T> {
    self.data.take()
  }

  pub(super) fn insert_data(&mut self, data: T) {
    self.data = Some(data);
  }
}

impl<T> Future for Op<T, SingleCQE>
where
  T: Unpin + 'static + Completable,
{
  type Output = T::Output;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let result = self
      .driver
      .upgrade()
      .expect("Not in runtime context")
      .poll_op(self.get_mut(), cx);

    CONTEXT.with(|x| x.call_on_thread_park());

    result
  }
}

impl<T> Future for Op<T, MultiCQE>
where
  T: Unpin + 'static + Completable + Updateable,
{
  type Output = T::Output;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let result = self
      .driver
      .upgrade()
      .expect("Not in runtime context")
      .poll_multishot_op(self.get_mut(), cx);

    CONTEXT.with(|x| x.call_on_thread_park());

    result
  }
}

impl<T> Future for Op<T, OneshotCQE>
where
  T: Unpin + 'static + Completable,
{
  type Output = T::Output;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let result = self
      .driver
      .upgrade()
      .expect("Not in runtime context")
      .poll_op_oneshot(self.get_mut(), cx);

    CONTEXT.with(|x| x.call_on_thread_park());

    result
  }
}

impl<D: 'static, T: OneshotOutputTransform<StoredData = D>> Completable for InFlightOneshot<D, T> {
  type Output = T::Output;

  fn complete(mut self, cqe: CqeResult) -> Self::Output {
    let inner = self.inner.take().unwrap();

    inner
      .post_op
      .transform_oneshot_output(inner.stable_data, cqe)
  }
}

/// The operation may have pending cqe's not yet processed.
/// To manage this, the lifecycle associated with the Op may if required
/// be placed in LifeCycle::Ignored state to handle cqe's which arrive after
/// the Op has been dropped.
impl<T: Unpin, CqeType: Unpin> Drop for Op<T, CqeType> {
  fn drop(&mut self) {
    self
      .driver
      .upgrade()
      .expect("Not in runtime context")
      .remove_op(self);
  }
}

impl Lifecycle {
  pub(crate) fn complete(&mut self, cqe: cqueue::Entry) {
    use std::mem;

    match mem::replace(self, Lifecycle::Submitted) {
      x @ Lifecycle::Initial | x @ Lifecycle::Submitted | x @ Lifecycle::Waiting(..) => {
        if cqueue::more(cqe.flags()) {
          let mut list = LinkedList::new();
          list.push_back(cqe.into());
          *self = Lifecycle::CompletionList(list);
        } else {
          *self = Lifecycle::Completed(cqe);
        }

        if let Lifecycle::Waiting(waker) = x {
          // waker is woken to notify cqe has arrived
          // Note: Maybe defer calling until cqe with !`more` flag set?
          waker.wake();
        }
      }

      lifecycle @ Lifecycle::Ignored(..) => {
        if cqueue::more(cqe.flags()) {
          // Not yet complete. The Op has been dropped, so we can drop the CQE
          // but we must keep the lifecycle alive until no more CQE's expected
          *self = lifecycle;
        }
      }

      Lifecycle::Completed(cqe) => {
        *self = Lifecycle::Completed(cqe);
      }
      Lifecycle::CompletionList(mut list) => {
        list.push_back(cqe.into());
        *self = Lifecycle::CompletionList(list);
      }
    }
  }
}
