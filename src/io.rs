//! Serving over a byte stream: newline-delimited JSON-RPC, as in the MCP
//! stdio transport. Works with stdin/stdout, sockets, pipes, child process
//! handles...

use crate::error::{Error, Result};
use crate::server::{Outbound, Outlet, Server, Session, dispatch_text};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// How long running requests get to finish once the input ends.
const EOF_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// A session served over a byte stream.
pub struct Connection {
    pub(crate) session: Session,
    pub(crate) task: JoinHandle<Result<()>>,
}

impl Connection {
    /// The session, to push notifications or channel events to the client.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Wait until the client disconnects (end of input) or the session is
    /// closed.
    pub async fn wait(self) -> Result<()> {
        self.task.await.map_err(|e| Error::Other(format!("connection task failed: {e}")))?
    }
}

impl Server {
    /// Serve one session over `reader`/`writer`, in the background.
    ///
    /// Messages are newline-delimited JSON. The session ends at the end of
    /// input (after giving running requests a few seconds to answer), on a
    /// write error, or when closed.
    pub fn connect_io<R, W>(&self, reader: R, writer: W) -> Connection
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let outlet = Outlet::Channel(tx);
        let session = Session::new(self.clone(), outlet.clone());
        let s = session.clone();
        let task = tokio::spawn(async move {
            let (stop_tx, stop_rx) = oneshot::channel();
            let writer = tokio::spawn(write_loop(writer, rx, stop_rx, s.clone()));
            let read = read_loop(reader, &s, &outlet).await;
            if read.is_ok() {
                // End of input: requests already received still get their
                // responses (think `cat requests.jsonl | server`).
                s.drain(EOF_GRACE).await;
            }
            s.close();
            let _ = stop_tx.send(());
            let write = writer.await.map_err(|e| Error::Other(format!("writer task failed: {e}")))?;
            read.and(write)
        });
        Connection { session, task }
    }

    /// Serve one session over `reader`/`writer` until it ends.
    pub async fn serve_io<R, W>(&self, reader: R, writer: W) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        self.connect_io(reader, writer).wait().await
    }

    /// Serve one session over stdin/stdout, in the background.
    ///
    /// Stdout then belongs to the protocol: log to stderr.
    #[cfg(feature = "stdio")]
    pub fn connect_stdio(&self) -> Connection {
        self.connect_io(tokio::io::stdin(), tokio::io::stdout())
    }

    /// Serve one session over stdin/stdout until the client goes away.
    ///
    /// Stdout then belongs to the protocol: log to stderr.
    #[cfg(feature = "stdio")]
    pub async fn serve_stdio(&self) -> Result<()> {
        self.connect_stdio().wait().await
    }
}

async fn read_loop<R: AsyncRead + Unpin>(reader: R, session: &Session, outlet: &Outlet) -> Result<()> {
    let mut reader = BufReader::new(reader);
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = tokio::select! {
            n = reader.read_until(b'\n', &mut line) => n?,
            _ = session.closed() => return Ok(()),
        };
        if n == 0 {
            return Ok(());
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        dispatch_text(&line, outlet, |msg, reply| session.handle(msg, reply));
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut rx: mpsc::UnboundedReceiver<Outbound>,
    mut stop: oneshot::Receiver<()>,
    session: Session,
) -> Result<()> {
    let result = async {
        loop {
            let out = tokio::select! {
                biased;
                out = rx.recv() => out,
                _ = &mut stop => {
                    // Flush what is already queued, then stop.
                    rx.close();
                    while let Some(out) = rx.recv().await {
                        write_one(&mut writer, out).await?;
                    }
                    return writer.flush().await;
                }
            };
            match out {
                Some(out) => write_one(&mut writer, out).await?,
                None => return writer.flush().await,
            }
        }
    }
    .await;
    if result.is_err() {
        session.close();
    }
    Ok(result?)
}

async fn write_one<W: AsyncWrite + Unpin>(writer: &mut W, out: Outbound) -> std::io::Result<()> {
    let Some(mut line) = out.to_json()? else {
        return Ok(());
    };
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await
}
