//! Serving over a byte stream: newline-delimited JSON-RPC, as in the MCP
//! stdio transport. Works with stdin/stdout, sockets, pipes, child process
//! handles...

use crate::error::{Error, Result};
use crate::jsonrpc;
use crate::server::{Outbound, Outlet, Server, Session};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// A session served over a byte stream.
pub struct Connection {
    session: Session,
    task: JoinHandle<Result<()>>,
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
    /// input, on a write error, or when closed.
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
        for msg in jsonrpc::decode(&line) {
            match msg {
                Ok(msg) => session.handle(msg, outlet),
                Err(error) => {
                    outlet.send(Outbound::Message(error));
                }
            }
        }
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
    let Outbound::Message(msg) = out else {
        return Ok(());
    };
    let mut line = serde_json::to_vec(&msg)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await
}
