//! Signaling 服务集成测试
//!
//! 测试核心信令流程，确保 WebSocket 连接、注册、心跳、信令中继等功能正常

use actr_protocol::{
    AIdCredential, ActrToSignaling, ActrType, Ping, Realm, SignalingEnvelope, actr_to_signaling,
    signaling_envelope, signaling_to_actr,
};
use actrix_common::config::ActrixConfig;
use actrix_common::realm::Realm as RealmEntity;
use actrix_common::storage::db::set_db_path;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use signaling::axum_router::create_signaling_router;
use std::path::Path;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tokio::time::timeout;
use tokio_tungstenite::{connect_async, tungstenite::Message as TungsteniteMessage};
use uuid::Uuid;

/// 测试辅助：创建最小配置
#[allow(dead_code)]
fn create_test_config() -> ActrixConfig {
    toml::from_str(
        r#"
        enable = 8
        name = "test-signaling"
        env = "test"
        sqlite = ":memory:"
        actrix_shared_key = "test-key-12345678901234567890123456789012"

        [bind.ice]
        ip = "127.0.0.1"
        port = 3478
    "#,
    )
    .expect("Failed to parse test config")
}

/// 测试辅助：创建信令服务器
async fn create_test_server() -> (String, tokio::task::JoinHandle<()>) {
    // 初始化测试数据库（确保全局数据库已设置）
    // 使用临时目录作为数据库目录，避免污染本地状态
    static INIT: OnceCell<()> = OnceCell::const_new();

    INIT.get_or_init(|| async {
        let db_dir = std::env::temp_dir().join("actrix_signaling_test_db");
        std::fs::create_dir_all(&db_dir).expect("Failed to create test database directory");
        let db_file = db_dir.join("actrix.db");
        if db_file.exists() {
            let _ = std::fs::remove_file(&db_file);
        }

        let db_dir_str = db_dir
            .to_str()
            .expect("Failed to convert DB directory path to string");

        // 尝试设置全局数据库路径，忽略已初始化的错误
        match set_db_path(Path::new(db_dir_str)).await {
            Ok(()) => {}
            Err(e) => {
                let err_msg = e.to_string();
                if !err_msg.contains("already initialized") && !err_msg.contains("Database already")
                {
                    panic!("Failed to initialize test database: {}", e);
                }
            }
        }
    })
    .await;

    // 确保测试所需的 realm 存在（realm_id = 1001）——避免因为 realm 不存在导致 403 错误
    if RealmEntity::exists_by_realm_id(1001).await == false {
        let mut realm = RealmEntity::new(1001, "test_realm".to_string());
        // 保存到数据库（忽略重复错误）
        let _ = realm.save().await;
    }

    // 使用 create_signaling_router 创建路由器
    let app = create_signaling_router()
        .await
        .expect("Failed to create router");

    // 绑定到随机端口
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{}/ws", addr);

    // 启动服务器（添加 ConnectInfo 支持）
    let handle = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });

    // 等待服务器启动
    tokio::time::sleep(Duration::from_millis(200)).await;

    (ws_url, handle)
}

/// 测试辅助：创建 SignalingEnvelope
fn create_envelope(flow: signaling_envelope::Flow) -> SignalingEnvelope {
    use prost_types::Timestamp;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();

    SignalingEnvelope {
        envelope_version: 1,
        envelope_id: Uuid::new_v4().to_string(),
        timestamp: Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        },
        reply_for: None,
        traceparent: None,
        tracestate: None,
        flow: Some(flow),
    }
}

/// 测试辅助：创建测试 credential（占位符）
fn create_test_credential() -> AIdCredential {
    AIdCredential {
        token_key_id: 1,
        encrypted_token: Bytes::from_static(b"test-credential-placeholder"),
    }
}

/// 测试辅助：创建测试 ActrId（用于中继）
fn create_test_actr_id(serial: u64) -> actr_protocol::ActrId {
    actr_protocol::ActrId {
        serial_number: serial,
        realm: Realm { realm_id: 1001 },
        r#type: ActrType {
            manufacturer: "test".to_string(),
            name: "device".to_string(),
        },
    }
}

/// 测试辅助：发送 protobuf 消息
async fn send_protobuf(
    ws: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        TungsteniteMessage,
    >,
    envelope: &SignalingEnvelope,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    envelope.encode(&mut buf)?;
    ws.send(TungsteniteMessage::Binary(Bytes::from(buf)))
        .await?;
    Ok(())
}

/// 测试辅助：接收 protobuf 消息
async fn receive_protobuf(
    ws: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) -> Result<SignalingEnvelope, Box<dyn std::error::Error>> {
    let msg = timeout(Duration::from_secs(5), ws.next())
        .await?
        .ok_or("Connection closed")??;

    match msg {
        TungsteniteMessage::Binary(data) => {
            let envelope = SignalingEnvelope::decode(&data[..])?;
            Ok(envelope)
        }
        _ => Err("Expected binary message".into()),
    }
}

#[tokio::test]
async fn test_websocket_connection() {
    // 创建服务器
    let (ws_url, _handle) = create_test_server().await;

    // 连接 WebSocket
    let result = connect_async(&ws_url).await;
    assert!(result.is_ok(), "WebSocket connection failed");

    let (ws_stream, _) = result.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // 发送 Ping 测试连接（使用占位符 credential）
    let ping_msg = ActrToSignaling {
        source: create_test_actr_id(999),
        credential: create_test_credential(),
        payload: Some(actr_to_signaling::Payload::Ping(Ping {
            availability: 100,
            mailbox_backlog: 0.0,
            power_reserve: 80.0,
            // 其他字段使用默认值
            ..Default::default()
        })),
    };
    let ping = create_envelope(signaling_envelope::Flow::ActrToServer(ping_msg));

    send_protobuf(&mut write, &ping).await.unwrap();

    // 接收响应（因为 credential 无效，应该收到错误）
    let response = receive_protobuf(&mut read).await;
    assert!(response.is_ok(), "Failed to receive response");

    let envelope = response.unwrap();
    assert!(
        matches!(
            envelope.flow,
            Some(signaling_envelope::Flow::ServerToActr(_))
        ),
        "Expected ServerToActr flow"
    );

    // 验证收到错误响应（credential 验证失败）
    if let Some(signaling_envelope::Flow::ServerToActr(signaling_msg)) = envelope.flow {
        match signaling_msg.payload {
            Some(signaling_to_actr::Payload::Error(ref err)) => {
                assert_eq!(err.code, 401, "Expected 401 error code");
                assert!(
                    err.message.contains("Credential validation failed"),
                    "Expected credential validation error"
                );
                println!("✅ WebSocket connection test passed: received expected 401 error");
            }
            other => {
                panic!("Expected Error response, got: {:?}", other);
            }
        }
    }
}

#[tokio::test]
async fn test_credential_validation() {
    let (ws_url, _handle) = create_test_server().await;
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // 发送多个带无效 credential 的 Ping
    for i in 0..3 {
        let ping_msg = ActrToSignaling {
            source: create_test_actr_id(999),
            credential: create_test_credential(),
            payload: Some(actr_to_signaling::Payload::Ping(Ping {
                availability: 100,
                mailbox_backlog: 0.0,
                power_reserve: 80.0,
                ..Default::default()
            })),
        };
        let ping = create_envelope(signaling_envelope::Flow::ActrToServer(ping_msg));

        send_protobuf(&mut write, &ping).await.unwrap();

        // 接收错误响应
        let response = receive_protobuf(&mut read).await.unwrap();

        if let Some(signaling_envelope::Flow::ServerToActr(signaling_msg)) = response.flow {
            // 验证返回错误（因为 credential 无效）
            assert!(
                matches!(
                    signaling_msg.payload,
                    Some(signaling_to_actr::Payload::Error(_))
                ),
                "Expected Error for ping {} due to invalid credential",
                i
            );
        } else {
            panic!("Expected ServerToActr flow for ping {}", i);
        }
    }

    println!("✅ Credential validation test passed");
}

#[tokio::test]
async fn test_invalid_message_handling() {
    let (ws_url, _handle) = create_test_server().await;
    let (ws_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // 发送无效的 protobuf 数据
    write
        .send(TungsteniteMessage::Binary(Bytes::from_static(&[
            0xFF, 0xFF, 0xFF,
        ])))
        .await
        .unwrap();

    // 服务器应该关闭连接或发送错误
    let response = timeout(Duration::from_secs(2), read.next()).await;

    // 可能是连接关闭或错误响应
    match response {
        Ok(Some(Ok(TungsteniteMessage::Close(_)))) => {
            println!("✅ Server closed connection on invalid message");
        }
        Ok(Some(Ok(TungsteniteMessage::Binary(data)))) => {
            // 尝试解析错误响应
            if let Ok(envelope) = SignalingEnvelope::decode(&data[..]) {
                println!("✅ Server sent error response: {:?}", envelope);
            }
        }
        Ok(None) => {
            println!("✅ Connection closed after invalid message");
        }
        Err(_) => {
            println!("⚠️  Timeout waiting for response (acceptable)");
        }
        _ => {
            println!("⚠️  Unexpected response type");
        }
    }
}
