//! Independent boundary and lifecycle regressions from the primary review.
#![cfg(unix)]

use std::time::Duration;
use threadlane::{
    RuntimeDescriptor, SecretToken, StateLock, Supervisor, SupervisorClient, SupervisorConfig,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn state_directory_cannot_follow_a_symlinked_parent() {
    let temp = tempfile::tempdir().unwrap();
    let real = temp.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let result = Supervisor::start(SupervisorConfig::new(link.join("state"))).await;
    if let Ok(handle) = result {
        handle.shutdown().await;
        panic!("state creation followed a symlinked parent");
    }
    assert!(!real.join("state").exists());
}

#[tokio::test]
async fn chunked_response_limit_is_enforced_before_the_response_finishes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 8192];
        let _ = stream.read(&mut request).await;
        stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let chunk = vec![b'x'; 65536];
        for _ in 0..34 {
            if stream.write_all(b"10000\r\n").await.is_err() {
                return;
            }
            if stream.write_all(&chunk).await.is_err() {
                return;
            }
            if stream.write_all(b"\r\n").await.is_err() {
                return;
            }
        }
        // Never send the terminating chunk: the byte budget must stop this read.
        std::future::pending::<()>().await;
    });
    let client = SupervisorClient::from_descriptor(RuntimeDescriptor {
        version: 1,
        host: "127.0.0.1".into(),
        port,
        pid: 1,
        token: SecretToken::new("a".repeat(64)),
    })
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), client.status("bnd-review")).await;
    server.abort();
    let error = result
        .expect("client waited for an unbounded response instead of enforcing its byte limit")
        .unwrap_err();
    assert!(error.to_string().contains("limit"), "{error}");
}

#[tokio::test]
async fn retired_supervisor_cannot_write_after_another_owner_acquires_the_lock() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let handle = Supervisor::start(SupervisorConfig::new(&state))
        .await
        .unwrap();
    let descriptor = handle.descriptor().clone();
    let body = r#"{"thread_uuid":"ab888b8c-28ab-4459-980f-017959f0da3c","label":"late request"}"#;
    let mut stream = TcpStream::connect(("127.0.0.1", descriptor.port))
        .await
        .unwrap();
    let request = format!("POST /v1/bindings HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", descriptor.token.as_str(), body.len());
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut interim = [0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut interim))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&interim[..n]).contains("100 Continue"));
    drop(handle);
    // Either ownership remains with the in-flight handler, or that handler must
    // already be incapable of mutating its old Core. Both are safe outcomes.
    let new_owner = StateLock::acquire(&state);
    if new_owner.is_ok() && stream.write_all(body.as_bytes()).await.is_ok() {
        let mut response = Vec::new();
        let _ =
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response)).await;
        assert!(
            !String::from_utf8_lossy(&response).starts_with("HTTP/1.1 201"),
            "old supervisor accepted a mutation after another owner acquired the state lock"
        );
    }
}
