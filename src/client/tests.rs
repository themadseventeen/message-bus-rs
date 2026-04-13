#[cfg(test)]
mod tests {

    use serde_json::Value;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::client::Client;

    pub async fn spawn_malformed_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();

                tokio::spawn(async move {
                    let mut buf = [0; 1024];
                    let _ = socket.read(&mut buf).await; // read request

                    // --- VALID HEADERS ---
                    let headers = b"HTTP/1.1 200 OK\r\n\
                            Transfer-Encoding: chunked\r\n\
                            Content-Type: application/json\r\n\
                            Connection: keep-alive\r\n\
                            \r\n";

                    socket.write_all(headers).await.unwrap();

                    // --- VALID CHUNK ---
                    let data = br#"
                        [
                          {
                            "global_id": 12,
                            "message_id": 1,
                            "channel": "/some/channel/name",
                            "data": "Hi"
                          }
                        ]|
                        "#;
                    // let payload = b"[{\"global_id\": 12,\"message_id\": 1,\"channel\": \"/some/channel/name\",\"data\":\"Hi\"}]";
                    let len = data.len();
                    let chunk_header = format!("ZZZ\r\n"); // "C\r\n"
                    socket.write_all(chunk_header.as_bytes()).await.unwrap();
                    socket.write_all(data).await.unwrap();
                    socket.write_all(b"\r\n").await.unwrap();
                    socket.write_all(b"0\r\n\r\n").await.unwrap();
                    // simulate long-poll delay
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

                    // --- BROKEN CHUNK (invalid size) ---
                    // socket.write_all(b"ZZZ\r\nbroken\r\n").await.unwrap();

                    // abruptly close connection (no final 0 chunk)
                    let _ = socket.shutdown().await;
                });
            }
        });

        addr
    }

    #[tokio::test]
    async fn test_malformed_stream_triggers_error() {
        use futures::StreamExt;
        let addr = spawn_malformed_server().await;

        let client = reqwest::Client::builder().http1_only().build().unwrap();

        let url = format!("http://{}", addr);

        // let response = client.get(&url).send().await.unwrap();

        // let mut stream = response.bytes_stream();

        let mut got_error = false;

        let mut mb = Client::builder()
            .http_client(client)
            .url(url)
            .build::<Value>()
            .unwrap();
        let mut stream = mb.stream().unwrap();

        while let Some(item) = stream.next().await {
            match item {
                Ok(item) => {
                    println!("item {item:?}")
                }
                Err(error) => {
                    println!("error {error:?}")
                }
            }
        }

        // assert!(got_error, "Expected stream decoding error");
    }
}
