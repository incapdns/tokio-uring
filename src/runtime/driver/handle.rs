//! Internal, reference-counted handle to the driver.
//!
//! The driver was previously managed exclusively by thread-local context, but this proved
//! untenable.
//!
//! The new system uses a handle which reference-counts the driver to track ownership and access to
//! the driver.
//!
//! There are two handles.
//! The strong handle is owning, and the weak handle is non-owning.
//! This is important for avoiding reference cycles.
//! The weak handle should be used by anything which is stored in the driver or does not need to
//! keep the driver alive for it's duration.

use crate::buf::fixed::FixedBuffers;
use crate::runtime::driver::op::{Completable, MultiCQE, OneshotCQE, Op, SingleCQE, Updateable};
use crate::runtime::driver::Driver;
use io_uring::squeue;
use std::cell::RefCell;
use std::io;
use std::ops::Deref;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

#[derive(Clone)]
pub(crate) struct Handle {
  pub(super) inner: Rc<Driver>,
}

//SAFETY: Handle is only used in RuntimeContext,
// which in turn is only accessed via thread_local
unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

#[derive(Clone)]
pub(crate) struct WeakHandle {
  inner: Weak<Driver>,
}

//SAFETY: Same as above
unsafe impl Send for WeakHandle {}
unsafe impl Sync for WeakHandle {}

impl Handle {
  pub(crate) fn new(b: &crate::Builder) -> io::Result<Self> {
    Ok(Self {
      inner: Rc::new(Driver::new(b)?),
    })
  }

  #[allow(dead_code)]
  pub(crate) fn weak(&self) -> WeakHandle {
    self.into()
  }

  pub(crate) fn dispatch_completions(&self) {
    self.inner.as_ref().dispatch_completions()
  }

  pub(crate) fn flush(&self) -> io::Result<usize> {
    self.inner.as_ref().uring.submit()
  }

  pub(crate) fn register_buffers(
    &mut self,
    buffers: Rc<RefCell<dyn FixedBuffers>>,
  ) -> io::Result<()> {
    let inner = Rc::get_mut(&mut self.inner).unwrap();
    inner.register_buffers(buffers)
  }

  pub(crate) fn unregister_buffers(&mut self) -> io::Result<()> {
    let inner = Rc::get_mut(&mut self.inner).unwrap();
    inner.unregister_buffers()
  }

  pub(crate) fn remove_op<T, CqeType: Unpin>(&self, op: &mut Op<T, CqeType>)
  where
    T: Unpin + 'static,
  {
    self.inner.as_ref().remove_op(op)
  }

  pub(crate) fn submit_op_2<T, S>(
    &self,
    sqe: squeue::Entry,
    data: T,
  ) -> io::Result<Pin<Box<Op<T, S>>>>
  where
    T: Completable,
    T: Unpin,
    S: Unpin,
  {
    self.inner.as_ref().submit_op_2(sqe, data, self.into())
  }

  pub(crate) fn submit_op<T, S, F>(&self, data: T, f: F) -> io::Result<Pin<Box<Op<T, S>>>>
  where
    T: Completable,
    F: FnOnce(&mut T) -> squeue::Entry,
    T: Unpin,
    S: Unpin,
  {
    self.inner.as_ref().submit_op(data, f, self.into())
  }

  pub(crate) fn poll_op<T>(
    &self,
    op: &mut Op<T, SingleCQE>,
    cx: &mut Context<'_>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable,
  {
    self.inner.as_ref().poll_op(op, cx)
  }

  pub(crate) fn poll_oneshot_op<T>(
    &self,
    op: &mut Op<T, OneshotCQE>,
    cx: &mut Context<'_>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable,
  {
    self.inner.as_ref().poll_op_oneshot(op, cx)
  }

  pub(crate) fn poll_multishot_op<T>(
    &self,
    op: &mut Op<T, MultiCQE>,
    cx: &mut Context<'_>,
  ) -> Poll<T::Output>
  where
    T: Unpin + 'static + Completable + Updateable,
  {
    self.inner.as_ref().poll_multishot_op(op, cx)
  }
}

impl WeakHandle {
  pub(crate) fn upgrade(&self) -> Option<Handle> {
    Some(Handle {
      inner: self.inner.upgrade()?,
    })
  }
}

impl AsRawFd for Handle {
  fn as_raw_fd(&self) -> RawFd {
    self.inner.as_ref().uring.as_raw_fd()
  }
}

impl From<Driver> for Handle {
  fn from(driver: Driver) -> Self {
    Self {
      inner: Rc::new(driver),
    }
  }
}

impl<T> From<T> for WeakHandle
where
  T: Deref<Target = Handle>,
{
  fn from(handle: T) -> Self {
    Self {
      inner: Rc::downgrade(&handle.inner),
    }
  }
}

impl AsRawFd for WeakHandle {
  fn as_raw_fd(&self) -> RawFd {
    let driver = unsafe { &*self.inner.as_ptr() };
    driver.as_raw_fd()
  }
}
