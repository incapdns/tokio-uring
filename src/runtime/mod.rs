use std::collections::LinkedList;
use std::future::Future;
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use tokio::io::unix::AsyncFd;
use tokio::sync::Notify;
use tokio::task::LocalSet;

mod context;
pub(crate) mod driver;

use crate::runtime::driver::Handle;
use crate::Builder;
pub(crate) use context::RuntimeContext;

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
  contexts: Arc<Mutex<LinkedList<Context>>>,

  /// Signal for new worker threads
  signal: Arc<Notify>,

  /// Builder for runtime contexts
  builder: Builder,
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
pub fn spawn<F>(task: F) -> tokio::task::JoinHandle<F::Output>
where
  F: Future + Send + 'static,
  F::Output: Send + 'static,
{
  tokio::spawn(task)
}

#[derive(Clone)]
struct Item {
  context: Arc<RuntimeContext>,
}

impl AsRawFd for Item {
  fn as_raw_fd(&self) -> RawFd {
    self.context.driver_fd()
  }
}

unsafe impl Send for Item {}
unsafe impl Sync for Item {}

impl Item {
  fn new(context: Arc<RuntimeContext>) -> Item {
    Item { context }
  }

  fn async_fd(&self) -> AsyncFd<Item> {
    AsyncFd::new(self.clone()).unwrap()
  }
}

impl Runtime {
  /// Creates a new tokio_uring runtime on the current thread.
  ///
  /// This takes the tokio-uring [`Builder`](crate::Builder) as a parameter.
  pub fn new(builder: &Builder) -> io::Result<Runtime> {
    let mut runtime = Runtime {
      local: ManuallyDrop::new(LocalSet::new()),
      tokio_rt: MaybeUninit::zeroed(),
      contexts: Default::default(),
      builder: builder.clone(),
      signal: Arc::new(Notify::new()),
    };

    let on_thread_start = runtime.create_on_thread_start_callback();

    let rt = tokio::runtime::Builder::new_multi_thread()
      .worker_threads(builder.threads)
      .on_thread_start(on_thread_start)
      .on_thread_park(Runtime::on_thread_park)
      .enable_all()
      .build()?;

    let tokio_rt = ManuallyDrop::new(rt);

    runtime.tokio_rt = MaybeUninit::new(tokio_rt);

    runtime.start_uring_wakes_task();

    CONTEXT.with(|x| {
      x.set_handle(Handle::new(builder).expect("Internal error"));
      x.set_on_thread_park(Runtime::on_thread_park);
      let mut lock = runtime.contexts.lock().unwrap();
      lock.push_back(x.clone());
    });

    runtime.signal.notify_one();

    Ok(runtime)
  }

  fn on_thread_park() {
    CONTEXT.with(|x| {
      let _ = x.handle().flush();
    });
  }

  fn create_on_thread_start_callback(&self) -> impl Fn() + Sync + 'static {
    let is_unique = |item: &Context, list: &LinkedList<Context>| {
      let item_fd = item.driver_fd();

      !list.iter().any(|i| i.driver_fd() == item_fd)
    };

    let contexts = self.contexts.clone();
    let builder = self.builder.clone();
    let signal = self.signal.clone();

    move || {
      CONTEXT.with(|cx| {
        let item = cx.clone();
        item.set_handle(Handle::new(&builder).expect("Internal error"));

        let mut lock = contexts.lock().unwrap();
        if is_unique(&item, &lock.clone()) {
          lock.push_back(cx.clone());
          signal.notify_one();
        }
      });
    }
  }

  async fn wait_event(item: Item) {
    let async_fd = item.async_fd();
    loop {
      let mut guard = async_fd.readable().await.unwrap();
      guard.get_inner().context.dispatch_completions();
      guard.clear_ready();
    }
  }

  fn start_uring_wakes_task(&self) {
    //SAFETY: It's always already initialized on Runtime::new method
    let tokio_rt = unsafe { self.tokio_rt.assume_init_ref() };
    let _guard = tokio_rt.enter();
    self.local.spawn_local(Runtime::drive_uring_wakes(
      self.contexts.clone(),
      self.signal.clone(),
      self.builder.threads,
    ));
  }

  async fn drive_uring_wakes(
    contexts: Arc<Mutex<LinkedList<Context>>>,
    signal: Arc<Notify>,
    threads: usize,
  ) {
    let mut total = 0;

    while total != threads + 1 {
      signal.notified().await;

      let mut guard = contexts.lock().unwrap();

      for i in guard.iter() {
        tokio::spawn(Runtime::wait_event(Item::new(i.clone())));
        total += 1;
      }

      guard.clear();
    }
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
          future.as_mut().poll(cx)
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
  use crate::builder;

  #[test]
  fn block_on() {
    let rt = Runtime::new(&builder()).unwrap();
    rt.block_on(async move { () });
  }

  #[test]
  fn block_on_twice() {
    let rt = Runtime::new(&builder()).unwrap();
    rt.block_on(async move { () });
    rt.block_on(async move { () });
  }
}
