#![cfg(feature = "arrow")]

use antalya_rs::{AntalyaClient, Config, Credentials, Endpoint, Outcome, PoolOptions, Query};
use std::{
    collections::BTreeMap,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestKind {
    Command,
    Flush,
    Observe,
}

type CapturedRequest = (usize, RequestKind, String);

struct PersistentServer {
    address: SocketAddr,
    endpoint: Endpoint,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    failures: Arc<Mutex<Vec<String>>>,
    thread: Option<JoinHandle<()>>,
}

impl PersistentServer {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = Endpoint::parse(&format!("http://{address}")).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let failures = Arc::new(Mutex::new(Vec::new()));
        let (stopping, connections, captured, errors) = (
            Arc::clone(&stop),
            Arc::clone(&sockets),
            Arc::clone(&requests),
            Arc::clone(&failures),
        );
        let thread = std::thread::spawn(move || {
            let mut workers = Vec::new();
            for accepted in listener.incoming() {
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                let socket = accepted.unwrap();
                socket.set_nodelay(true).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let connection = {
                    let mut sockets = connections.lock().unwrap();
                    let connection = sockets.len();
                    sockets.push(socket.try_clone().unwrap());
                    connection
                };
                let (stopping, captured, errors) = (
                    Arc::clone(&stopping),
                    Arc::clone(&captured),
                    Arc::clone(&errors),
                );
                workers.push(std::thread::spawn(move || {
                    if let Err(error) = serve(socket, connection, &captured)
                        && !stopping.load(Ordering::Acquire)
                    {
                        errors.lock().unwrap().push(error.to_string());
                    }
                }));
            }
            for worker in workers {
                if worker.join().is_err() {
                    errors
                        .lock()
                        .unwrap()
                        .push("connection worker panicked".into());
                }
            }
        });
        Self {
            address,
            endpoint,
            stop,
            sockets,
            requests,
            failures,
            thread: Some(thread),
        }
    }
}

impl Drop for PersistentServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(sockets) = self.sockets.lock() {
            for socket in sockets.iter() {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }
        // Wake blocking accept; read/write deadlines also bound a racing new worker.
        let _ = TcpStream::connect_timeout(&self.address, Duration::from_secs(1));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn request(reader: &mut BufReader<TcpStream>) -> io::Result<Option<(Vec<u8>, String)>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut length = None;
    let mut authorization = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        if line == "\r\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if lower.starts_with("authorization:") {
            authorization = line["authorization:".len()..].trim().to_owned();
        }
        if let Some(value) = lower.strip_prefix("content-length:") {
            length = Some(value.trim().parse::<usize>().map_err(io::Error::other)?);
        }
        if lower.starts_with("transfer-encoding:") {
            return Err(io::Error::other(
                "fixture requires the clients' fixed-length request bodies",
            ));
        }
    }
    let length = length.ok_or_else(|| io::Error::other("missing request content length"))?;
    if length > 64 * 1024 {
        return Err(io::Error::other("fixture request exceeds bound"));
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(Some((body, authorization)))
}

fn serve(
    socket: TcpStream,
    connection: usize,
    requests: &Mutex<Vec<CapturedRequest>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(socket);
    while let Some((body, authorization)) = request(&mut reader)? {
        let sql = String::from_utf8(body).map_err(io::Error::other)?;
        let (kind, reply): (_, &[u8]) = if sql.contains("FROM system.query_log") {
            (RequestKind::Observe, br#"{"data":[{"starts":1,"finishes":1,"errors":0,"cancelled":0,"result_rows":0,"read_rows":0,"read_bytes":0}]}"#)
        } else if sql.trim() == "SYSTEM FLUSH LOGS query_log" {
            (RequestKind::Flush, b"")
        } else if sql.trim() == "CREATE DATABASE pool_fixture" {
            (RequestKind::Command, b"")
        } else {
            return Err(io::Error::other("unexpected fixture SQL"));
        };
        requests
            .lock()
            .unwrap()
            .push((connection, kind, authorization));
        write!(
            reader.get_mut(),
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            reply.len()
        )?;
        reader.get_mut().write_all(reply)?;
        reader.get_mut().flush()?;
    }
    Ok(())
}

async fn check_pool(
    max_idle_per_host: usize,
    expected_requests_per_connection: &[usize],
    rotate: bool,
) {
    let server = PersistentServer::new();
    let client = AntalyaClient::new(
        Config::new(
            server.endpoint.clone(),
            Credentials::bearer("token").unwrap(),
        )
        .timeout(Duration::from_secs(5))
        .pool(PoolOptions {
            max_idle_per_host,
            idle_timeout: Duration::from_secs(2),
        }),
    )
    .unwrap();
    let clone = if rotate {
        client.with_bearer("rotated").unwrap()
    } else {
        client.clone()
    };
    let clients = if rotate {
        vec![&client, &clone, &client]
    } else {
        vec![&client, &clone]
    };
    let rounds = clients.len();
    for (index, client) in clients.into_iter().enumerate() {
        // Each completed command includes two network round trips on the control
        // client, allowing the separate upstream connection to return to its pool.
        let report = client
            .execute_command(
                Query::new("CREATE DATABASE pool_fixture", format!("pool-{index}")).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(report.outcome, Outcome::Succeeded);
        assert_eq!(report.metrics.flush_attempts, 1);
        assert_eq!(report.metrics.polls, 1);
    }
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), rounds * 3);
    for (index, (_, _, authorization)) in requests.iter().enumerate() {
        let expected = if rotate && index / 3 == 1 {
            "Bearer rotated"
        } else {
            "Bearer token"
        };
        assert_eq!(authorization, expected);
    }
    for kind in [
        RequestKind::Command,
        RequestKind::Flush,
        RequestKind::Observe,
    ] {
        assert_eq!(
            requests
                .iter()
                .filter(|(_, actual, _)| *actual == kind)
                .count(),
            rounds
        );
    }
    let mut by_connection = BTreeMap::new();
    for (connection, _, _) in requests {
        *by_connection.entry(connection).or_insert(0usize) += 1;
    }
    let mut counts: Vec<_> = by_connection.into_values().collect();
    counts.sort_unstable();
    assert_eq!(counts, expected_requests_per_connection);
    assert_eq!(server.sockets.lock().unwrap().len(), counts.len());
    assert!(server.failures.lock().unwrap().is_empty());
}

#[tokio::test]
async fn configured_pool_reuses_upstream_and_control_connections_across_clones() {
    check_pool(2, &[2, 4], false).await;
}

#[tokio::test]
async fn disabling_idle_pool_disables_upstream_and_control_reuse() {
    check_pool(0, &[1, 1, 1, 1, 1, 1], false).await;
}

#[tokio::test]
async fn bearer_replacement_reuses_both_pools_and_keeps_original_view_usable() {
    check_pool(2, &[3, 6], true).await;
}
