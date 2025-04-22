use std::{io, io::Write};

use tokio_uring::fs::File;

fn main() {
  tokio_uring::start(async move {
    tokio_uring::spawn(async move {
      // Lock stdout
      let mut out = io::stdout();

      let path = "/etc/hosts";

      // Open the file without blocking
      let file = File::open(path).await.unwrap();
      let mut buf = vec![0; 16 * 1_024];

      // Track the current position in the file;
      let mut pos = 0;

      loop {
        // Read a chunk
        let (res, b) = file.read_at(buf, pos).await;
        let n = res.unwrap();

        if n == 0 {
          break;
        }

        out.write_all(&b[..n]).unwrap();
        pos += n as u64;

        buf = b;
      }

      // Include a new line
      println!();
    })
    .await
    .unwrap()
  });
}
