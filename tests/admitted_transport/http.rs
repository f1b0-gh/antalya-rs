//! Bounded two-request HTTP fixture: identity first, scripted data response next.
use antalya_rs::{
    AntalyaClient, AuthorizationContext, AuthorizedRequest, Config, Credentials, Endpoint,
    ExecutionLimits, Format, Query,
};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct Server {
    endpoint: Endpoint,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
}
impl Server {
    pub fn new(reply: Vec<u8>) -> Result<Self> {
        Self::fragmented(reply, usize::MAX, Duration::ZERO, None)
    }
    pub fn fragmented(
        reply: Vec<u8>,
        chunk_size: usize,
        delay: Duration,
        mut pause: Option<(usize, std::sync::mpsc::Receiver<()>)>,
    ) -> Result<Self> {
        if chunk_size == 0 {
            return Err(io::Error::other("empty fixture chunk").into());
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = Endpoint::parse(&format!("http://{}", listener.local_addr()?))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            let identity = br#"{"data":[{"principal":"fixture"}]}"#;
            for index in 0..2 {
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut socket = loop {
                    if stopped.load(Ordering::Acquire) {
                        return Ok(());
                    }
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(e) => return Err(e),
                    }
                };
                socket.set_read_timeout(Some(Duration::from_secs(2)))?;
                read_request(&mut socket)?;
                if index == 0 {
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        identity.len()
                    )?;
                    socket.write_all(identity)?;
                } else {
                    let mut sent = 0;
                    for chunk in reply.chunks(chunk_size) {
                        socket.write_all(chunk)?;
                        sent += chunk.len();
                        if pause.as_ref().is_some_and(|(at, _)| sent >= *at)
                            && let Some((_, release)) = pause.take()
                        {
                            release
                                .recv_timeout(Duration::from_secs(2))
                                .map_err(io::Error::other)?;
                        }
                        if !delay.is_zero() {
                            std::thread::sleep(delay);
                        }
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            stop,
            thread: Some(thread),
        })
    }
    pub async fn admit(&self, max_bytes: u64) -> Result<antalya_rs::AdmittedResponse> {
        self.admit_limits(ExecutionLimits {
            max_total_bytes: max_bytes,
            max_batch_bytes: 1,
            ..ExecutionLimits::default()
        })
        .await
    }
    pub async fn admit_limits(
        &self,
        limits: ExecutionLimits,
    ) -> Result<antalya_rs::AdmittedResponse> {
        let client = AntalyaClient::new(
            Config::new(self.endpoint.clone(), Credentials::bearer("token")?)
                .timeout(Duration::from_secs(2))
                .execution_limits(limits),
        )?;
        let request = AuthorizedRequest::new(
            Query::new("SELECT v", uuid::Uuid::new_v4().to_string())?,
            self.endpoint.clone(),
            Format::ArrowStream,
            AuthorizationContext {
                receipt_id: "fixture".into(),
                expected_principal: "fixture".into(),
            },
        )?;
        Ok(client
            .prepare_authorized_arrow(request)
            .await?
            .ready_transport()
            .await?
            .admit()?)
    }
    pub fn finish(mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("fixture thread panicked"))??;
        }
        Ok(())
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn read_request(socket: &mut TcpStream) -> io::Result<()> {
    let mut request = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let n = socket.read(&mut chunk)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "fixture request incomplete",
            ));
        }
        if request.len() + n > 16 * 1024 {
            return Err(io::Error::other("fixture request limit"));
        }
        request.extend_from_slice(&chunk[..n]);
        if let Some(end) = request.windows(4).position(|v| v == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]);
            let mut length = None;
            for line in headers.lines() {
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = Some(
                        value
                            .trim()
                            .parse::<usize>()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                    );
                }
            }
            let length =
                length.ok_or_else(|| io::Error::other("fixture requires content length"))?;
            if length > 8192 {
                return Err(io::Error::other("fixture body limit"));
            }
            if request.len() >= end + 4 + length {
                return Ok(());
            }
        }
    }
}
