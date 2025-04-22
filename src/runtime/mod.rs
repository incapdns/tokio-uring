use std::collections::LinkedList;
use std::future::Future;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::fd::AsRawFd;
use std::sync::{Arc, RwLock};
use tokio::io::unix::AsyncFd;
use tokio::task::LocalSet;
use tokio::sync::Notify;

mod context;
pub(crate) mod driver;

pub(crate) use context::RuntimeContext;
use crate::runtime::driver::Handle;

type Context = Arc<RuntimeContext>;

thread_local! {
    pub(crate) static CONTEXT: Context = Arc::new(RuntimeContext::new());
}

/// The Runtime Executor
///
/// This is the Runtime for `tokio-uring`.
/// It wraps the default [`Runtime`] using the platform-specific Driver.
///
/// This executes futures and tasks within the current-thread only.
///
/// [`Runtime`]: tokio::runtime::Runtime
pub struct Runtime {
  /// Tokio runtime, always current-thread
  tokio_rt: MaybeUninit<ManuallyDrop<tokio::runtime::Runtime>>,

  /// LocalSet for !Send tasks
  local: ManuallyDrop<LocalSet>,

  /// Linked List of all runtime contexts
  contexts: Arc<RwLock<LinkedList<Context>>>,

  /// Signal for new runtime context
  signal: Arc<Notify>,
}

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// Spawning a task enables the task to execute concurrently to other tasks.
/// There is no guarantee that a spawned task will execute to completion. When a
/// runtime is shutdown, all outstanding tasks are dropped, regardless of the
/// lifecycle of that task.
///
/// This function must be called from the context of a `tokio-uring` runtime.
///
/// [`JoinHandle`]: tokio::task::JoinHandle
///
/// # Examples
///
/// In this example, a server is started and `spawn` is used to start a new task
/// that processes each received connection.
///
/// ```no_run
/// tokio_uring::start(async {
///     let handle = tokio_uring::spawn(async {
///         println!("hello from a background task");
///     });
///
///     // Let the task complete
///     handle.await.unwrap();
/// });
/// ```
pub fn spawn<F: Future + 'static>(task: F) -> tokio::task::JoinHandle<F::Output>
where F: Future + Send + 'static,
      F::Output: Send + 'static
{
  tokio::spawn(task)
}

struct Item {
  _context: Arc<RuntimeContext>,
  async_fd: AsyncFd<Handle>
}

unsafe impl Send for Item {}
unsafe impl Sync for Item {}

impl Item {
  fn new(context: Arc<RuntimeContext>) -> Item {
    let handle = context.handle();

    Item {
      _context: context,
      async_fd: AsyncFd::new(handle).unwrap()
    }
  }
}

impl Runtime {
  /// Creates a new tokio_uring runtime on the current thread.
  ///
  /// This takes the tokio-uring [`Builder`](crate::Builder) as a parameter.
  pub fn new<'a>() -> io::Result<Runtime> {
    let mut runtime = Runtime {
      local: ManuallyDrop::new(LocalSet::new()),
      tokio_rt: MaybeUninit::zeroed(),
      contexts: Default::default(),
      signal: Arc::new(Notify::new()),
    };

    let on_thread_start = runtime.create_on_thread_start_callback();

    let rt = tokio::runtime::Builder::new_multi_thread()
      .on_thread_start(on_thread_start)
      .on_thread_park(|| {
        CONTEXT.with(|x| {
          let _ = x
            .handle()
            .flush();
        });
      })
      .enable_all()
      .build()?;

    let tokio_rt = ManuallyDrop::new(rt);

    runtime.tokio_rt = MaybeUninit::new(tokio_rt);

    runtime.start_uring_wakes_task();

    CONTEXT.with(|cx| {
      let mut lock = runtime.contexts.write().unwrap();
      lock.push_back(cx.clone());
    });

    Ok(runtime)
  }

  fn create_on_thread_start_callback(&self) -> impl Fn() + Sync + 'static {
    let is_unique = |item: &Arc<RuntimeContext>, list: LinkedList<Arc<RuntimeContext>>| {
      let item_fd = item.handle().as_raw_fd();

      list
        .iter()
        .find(|i| i.handle().as_raw_fd() == item_fd)
        .is_none()
    };

    let contexts = self.contexts.clone();

    let signal = self.signal.clone();

    move ||{
      let mut lock = contexts.write().unwrap();

      CONTEXT.with(|cx| {
        let item = cx.clone();

        if is_unique(&item, lock.clone()) {
          lock.push_back(cx.clone());
        }
      });

      signal.notify_one();
    }
  }

  async fn wait_event(item: Arc<Item>){
    loop {
      let mut guard = item.async_fd.readable().await.unwrap();
      guard.get_inner().dispatch_completions();
      guard.clear_ready();
    }
  }

  async fn drive_uring_wakes(signal: Arc<Notify>, contexts: Arc<RwLock<LinkedList<Arc<RuntimeContext>>>>) {
    let mut our_list: Vec<Arc<Item>> = Vec::with_capacity(255);

    loop {
      let guard = contexts.read().unwrap();

      let vec = guard.iter().collect::<Vec<_>>();
      let vec_len = vec.len();
      let our_list_len = our_list.len();

      if our_list_len != vec_len {
        vec
          .iter()
          .skip(our_list_len)
          .for_each(|rc|{
            our_list.push(Arc::new(Item::new((*rc).clone())));
          });

        for item in
          our_list
            .iter()
            .skip(our_list_len)
        {
          tokio::spawn(Runtime::wait_event(item.clone()));
        }
      }

      // Wait for signal
      signal.notified().await;
    }
  }

  fn start_uring_wakes_task(&self) {
    //SAFETY: It's always already initialized on Runtime::new method
    let _guard = unsafe { &*self.tokio_rt.as_ptr() }.enter();
    let future = Runtime::drive_uring_wakes(self.signal.clone(), self.contexts.clone());
    self.local.spawn_local(future);
  }

  /// Runs a future to completion on the tokio-uring runtime. This is the
  /// runtime's entry point.
  ///
  /// This runs the given future on the current thread, blocking until it is
  /// complete, and yielding its resolved result. Any tasks, futures, or timers
  /// which the future spawns internally will be executed on this runtime.
  ///
  /// Any spawned tasks will be suspended after `block_on` returns. Calling
  /// `block_on` again will resume previously spawned tasks.
  ///
  /// # Panics
  ///
  /// This function panics if the provided future panics, or if called within an
  /// asynchronous execution context.
  /// Runs a future to completion on the current runtime.
  pub fn block_on<F>(&self, future: F) -> F::Output
  where
    F: Future,
  {
    tokio::pin!(future);

    //SAFETY: It's always already initialized on Runtime::new method
    let res = unsafe {
      self
        .tokio_rt
        .assume_init_read()
        .block_on(self.local.run_until(std::future::poll_fn(|cx| {
          // assert!(drive.as_mut().poll(cx).is_pending());
          let result = future.as_mut().poll(cx);
          CONTEXT.with(|x| {
            let _ = x
              .handle()
              .flush();
          });
          result
        })))
    };

    res
  }
}

impl Drop for Runtime {
  fn drop(&mut self) {
    // drop tasks in correct order
    unsafe {
      let tokio_rt = self.tokio_rt.assume_init_mut();
      ManuallyDrop::drop(&mut self.local);
      ManuallyDrop::drop(tokio_rt);
    }
  }
}

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn block_on() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move { () });
  }

  #[test]
  fn block_on_twice() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move { () });
    rt.block_on(async move { () });
  }
}