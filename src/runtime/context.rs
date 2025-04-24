use crate::runtime::driver::{Handle, WeakHandle};
use std::cell::{Cell, OnceCell, RefCell, RefMut};
use std::ops::{Deref, DerefMut};
use std::os::fd::{AsRawFd, RawFd};

fn noop() {}

pub(crate) struct RefOnceCell<T> {
  cell: OnceCell<T>,
}

impl<T> Default for RefOnceCell<T> {
  fn default() -> Self {
    Self {
      cell: OnceCell::new(),
    }
  }
}

impl<T> Deref for RefOnceCell<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    self.cell.get().unwrap()
  }
}

impl<T> DerefMut for RefOnceCell<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.cell.get_mut().unwrap()
  }
}

/// Owns the driver and resides in thread-local storage.
pub(crate) struct RuntimeContext {
  driver: RefCell<RefOnceCell<Handle>>,
  on_thread_park: Cell<fn()>,
}

impl RuntimeContext {
  /// Construct the context with an uninitialized driver.
  pub(crate) fn new() -> RuntimeContext {
    RuntimeContext {
      driver: RefCell::new(Default::default()),
      on_thread_park: Cell::new(noop),
    }
  }

  pub(crate) fn handle(&self) -> RefMut<'_, RefOnceCell<Handle>> {
    self.driver.borrow_mut()
  }

  pub(crate) fn driver_fd(&self) -> RawFd {
    let roc = unsafe { &*self.driver.as_ptr() };
    roc.as_raw_fd()
  }

  pub(crate) fn dispatch_completions(&self) {
    let roc = unsafe { &*self.driver.as_ptr() };
    roc.dispatch_completions()
  }

  #[inline(always)]
  pub(crate) fn call_on_thread_park(&self) {
    self.on_thread_park.get()();
  }

  pub(crate) fn set_on_thread_park(&self, callback: fn()) {
    self.on_thread_park.set(callback)
  }

  /// Initialize the driver.
  pub(crate) fn set_handle(&self, handle: Handle) {
    let guard = self.driver.borrow_mut();

    let _ = guard.cell.set(handle);
  }

  pub(crate) fn unset_driver(&self) {
    let mut guard = self.driver.borrow_mut();

    guard.cell.take();
  }

  /// Check if driver is initialized
  #[allow(dead_code)]
  pub(crate) fn is_set(&self) -> bool {
    self
      .driver
      .try_borrow()
      .map(|b| b.cell.get().is_some())
      .unwrap_or(false)
  }

  #[allow(dead_code)]
  pub(crate) fn weak(&self) -> Option<WeakHandle> {
    self.driver.borrow().cell.get().map(Into::into)
  }
}

impl Drop for RuntimeContext {
  fn drop(&mut self) {
    self.unset_driver();
  }
}

//SAFETY: RuntimeContext is only accessed via thread_local
unsafe impl Send for RuntimeContext {}
unsafe impl Sync for RuntimeContext {}
