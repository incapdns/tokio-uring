use crate::io::SharedFd;
use crate::runtime::driver::op::{Completable, CqeResult, Op};
use crate::runtime::CONTEXT;
use io_uring::{opcode, types};
use std::io;
use std::pin::Pin;

pub(crate) struct Fsync {
  fd: SharedFd,
}

impl Op<Fsync> {
  pub(crate) fn fsync(fd: &SharedFd) -> io::Result<Pin<Box<Op<Fsync>>>> {
    CONTEXT.with(|x| {
      x.handle()
        .submit_op(Fsync { fd: fd.clone() }, |fsync| {
          opcode::Fsync::new(types::Fd(fsync.fd.raw_fd())).build()
        })
    })
  }

  pub(crate) fn datasync(fd: &SharedFd) -> io::Result<Pin<Box<Op<Fsync>>>> {
    CONTEXT.with(|x| {
      x.handle()
        .submit_op(Fsync { fd: fd.clone() }, |fsync| {
          opcode::Fsync::new(types::Fd(fsync.fd.raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
        })
    })
  }
}

impl Completable for Fsync {
  type Output = io::Result<()>;

  fn complete(self, cqe: CqeResult) -> Self::Output {
    cqe.result.map(|_| ())
  }
}
