use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

pub struct Server {
    pub endpoint: antalya_rs::Endpoint,
    pub requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    pub fn new(body: Vec<u8>, result_rows: u64, missing_first: bool, flush_status: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint =
            antalya_rs::Endpoint::parse(&format!("http://{}", listener.local_addr().unwrap()))
                .unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let thread = std::thread::spawn(move || {
            let mut observations = 0;
            while !stopping.load(Ordering::Acquire) {
                let (mut socket, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(e) => panic!("accept: {e}"),
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 4096];
                while let Ok(n) = socket.read(&mut chunk) {
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..end]);
                        let size = headers
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= end + 4 + size {
                            break;
                        }
                    }
                }
                let request = String::from_utf8_lossy(&bytes).into_owned();
                captured.lock().unwrap().push(request.clone());
                let (status, reply) = if request.contains("system.query_log") {
                    observations += 1;
                    let present = u8::from(!missing_first || observations > 1);
                    (200, format!(r#"{{"data":[{{"starts":{present},"finishes":{present},"errors":0,"cancelled":0,"result_rows":{result_rows},"read_rows":{result_rows},"read_bytes":8}}]}}"#).into_bytes())
                } else if request.contains("SYSTEM FLUSH LOGS") {
                    (flush_status, Vec::new())
                } else if request.contains("currentUser() AS principal") {
                    (200, br#"{"data":[{"principal":"fixture"}]}"#.to_vec())
                } else {
                    (200, body.clone())
                };
                let _ = write!(
                    socket,
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                );
                let _ = socket.write_all(&reply);
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
pub fn stream(batches: usize) -> Vec<u8> {
    let batch = arrow_array::RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(arrow_array::Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let mut body = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &batch.schema()).unwrap();
    for _ in 0..batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    body
}
