use serde::{Deserialize, Serialize};

/// TURN 服务配置
///
/// TURN 中继服务的专用配置参数。
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct TurnConfig {
    /// 中继端口范围
    ///
    /// TURN 服务用于数据中继的 UDP 端口范围。
    /// 格式：开始端口-结束端口，如 "49152-65535"。
    /// 范围越大，可支持的并发中继会话越多。
    pub relay_port_range: String,

    /// TURN 认证域
    ///
    /// TURN 服务的认证域名，用于 TURN 协议的认证机制。
    pub realm: String,

    /// TURN 共享密钥（HMAC 时效凭证）
    ///
    /// 用于生成和验证 TURN 时效凭证（coturn --use-auth-secret 兼容格式）。
    /// AIS 生成 TurnCredential 时使用此密钥，TURN 服务器验证时使用相同密钥。
    #[serde(default = "default_turn_secret")]
    pub turn_secret: String,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            relay_port_range: "49152-65535".to_string(),
            realm: "actrix.local".to_string(),
            turn_secret: default_turn_secret(),
        }
    }
}

fn default_turn_secret() -> String {
    "actrix-turn-secret-change-in-production".to_string()
}
