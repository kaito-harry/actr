//! WebRTC negotiator
//!
//! Responsible for WebRTC Connect's Offer/Answer protocol quotient

use crate::lifecycle::CredentialState;
use crate::transport::NetworkResult;
use actr_protocol::IceCandidate;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use std::sync::Arc;
use tokio::sync::OnceCell;
use webrtc::ice::udp_mux::UDPMux;
#[cfg(feature = "test-utils")]
use webrtc::util::vnet::net::Net;

// Re-export types from actr-config
pub use actr_config::{IceTransportPolicy, WebRtcConfig};

/// WebRTC negotiator
#[derive(Clone)]
pub(crate) struct WebRtcNegotiator {
    /// Base WebRTC configuration (URLs + policy)
    config: WebRtcConfig,
    /// Latest credential state (updated on register/renew, contains TurnCredential)
    credential_state: CredentialState,
    /// Process-shared ICE UDP mux, used only when `ice_udp_mux_port` is
    /// configured (opt-in): a single UDP socket reused by every
    /// RTCPeerConnection this negotiator creates, so ICE gathering no longer
    /// opens a fresh socket per interface per connection. Lazily bound on
    /// first use; shared across clones via the outer `Arc`.
    udp_mux: Arc<OnceCell<Arc<dyn UDPMux + Send + Sync>>>,
    /// Optional virtual network for integration testing.
    /// When set, RTCPeerConnection will use this VNet instead of real OS networking.
    #[cfg(feature = "test-utils")]
    vnet: Option<Arc<Net>>,
}

impl WebRtcNegotiator {
    ///
    /// # Arguments
    /// - `config`: WebRTC configuration
    /// - `credential_state`: shared credential state containing TurnCredential
    pub fn new(config: WebRtcConfig, credential_state: CredentialState) -> Self {
        if config.advanced.ice_udp_mux_port.is_some() && config.advanced.udp_ports.is_some() {
            tracing::warn!(
                ice_udp_mux_port = ?config.advanced.ice_udp_mux_port,
                udp_ports = ?config.advanced.udp_ports,
                "Both ice_udp_mux_port and udp_ports (port range) are configured; \
                 they are mutually exclusive — the shared ICE UDP mux takes \
                 precedence and the UDP port range is ignored"
            );
        }
        Self {
            config,
            credential_state,
            udp_mux: Arc::new(OnceCell::new()),
            #[cfg(feature = "test-utils")]
            vnet: None,
        }
    }

    /// Get (or lazily bind) the process-shared ICE UDP mux. A single UDP socket
    /// is bound on the configured `ice_udp_mux_port` the first time this is
    /// called and reused by every subsequent RTCPeerConnection, so the
    /// process's UDP socket count stays O(1) instead of growing
    /// one-per-interface per connection.
    ///
    /// Lifecycle: the mux socket (and its read task) is held for the lifetime
    /// of the coordinator that owns this negotiator; `UDPMux::close` is never
    /// called. ICE restarts change the local ufrag, and agent close only
    /// removes the current ufrag from the mux, so stale ufrag entries can
    /// accumulate in the mux across many restarts — bounded memory growth,
    /// no fd growth.
    async fn shared_udp_mux(&self, port: u16) -> NetworkResult<Arc<dyn UDPMux + Send + Sync>> {
        use webrtc::ice::udp_mux::{UDPMuxDefault, UDPMuxParams};

        let mux = self
            .udp_mux
            .get_or_try_init(|| async {
                let socket = tokio::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, port))
                    .await
                    .map_err(|e| {
                        crate::transport::NetworkError::Other(anyhow::anyhow!(
                            "failed to bind shared ICE UDP mux socket: {e}"
                        ))
                    })?;
                let local = socket.local_addr().ok();
                let mux: Arc<dyn UDPMux + Send + Sync> =
                    UDPMuxDefault::new(UDPMuxParams::new(socket));
                tracing::info!(
                    ?local,
                    "🔌 Bound shared ICE UDP mux (one socket for all peers)"
                );
                Ok::<_, crate::transport::NetworkError>(mux)
            })
            .await?;
        Ok(mux.clone())
    }

    /// Set the virtual network for testing.
    ///
    /// When set, all RTCPeerConnections created by this negotiator will use
    /// the provided VNet instead of the real OS network stack. This enables
    /// simulating network disconnection/reconnection at the UDP transport level.
    ///
    /// # Arguments
    /// - `vnet`: The virtual network instance (from `webrtc_util::vnet::net::Net`)
    #[cfg(feature = "test-utils")]
    pub fn set_vnet(&mut self, vnet: Arc<Net>) {
        tracing::info!("🌐 VNet injected into WebRtcNegotiator");
        self.vnet = Some(vnet);
    }

    /// Create RTCPeerConnection
    ///
    /// # Arguments
    /// - `is_answerer`: true if this node is the answerer (passive side), false if offerer (default)
    /// - `remote_fixed`: true if remote peer has fixed network configuration
    ///
    /// # Returns
    /// newCreate's PeerConnection
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip(self), fields(ice_servers = self.config.ice_servers.len(), is_answerer, remote_fixed))
    )]
    pub async fn create_peer_connection(
        &self,
        is_answerer: bool,
        remote_fixed: bool,
    ) -> NetworkResult<RTCPeerConnection> {
        use webrtc::api::APIBuilder;
        use webrtc::api::media_engine::MediaEngine;
        use webrtc::ice_transport::ice_server::RTCIceServer;
        use webrtc::peer_connection::configuration::RTCConfiguration;
        use webrtc::peer_connection::policy::ice_transport_policy::RTCIceTransportPolicy;
        use webrtc::rtp_transceiver::rtp_codec::{
            RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
        };

        // Create MediaEngine and register codecs
        let mut media_engine = MediaEngine::default();

        // Register VP8 video codec
        media_engine.register_codec(
            RTCRtpCodecParameters {
                capability: RTCRtpCodecCapability {
                    mime_type: "video/VP8".to_owned(),
                    clock_rate: 90000,
                    channels: 0,
                    sdp_fmtp_line: "".to_owned(),
                    rtcp_feedback: vec![],
                },
                payload_type: 96,
                ..Default::default()
            },
            RTPCodecType::Video,
        )?;

        // Register H264 video codec
        media_engine.register_codec(
            RTCRtpCodecParameters {
                capability: RTCRtpCodecCapability {
                    mime_type: "video/H264".to_owned(),
                    clock_rate: 90000,
                    channels: 0,
                    sdp_fmtp_line:
                        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                            .to_owned(),
                    rtcp_feedback: vec![],
                },
                payload_type: 102,
                ..Default::default()
            },
            RTPCodecType::Video,
        )?;

        // Register OPUS audio codec
        media_engine.register_codec(
            RTCRtpCodecParameters {
                capability: RTCRtpCodecCapability {
                    mime_type: "audio/opus".to_owned(),
                    clock_rate: 48000,
                    channels: 2,
                    sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(),
                    rtcp_feedback: vec![],
                },
                payload_type: 111,
                ..Default::default()
            },
            RTPCodecType::Audio,
        )?;

        // Get TURN credentials (time-limited HMAC credentials issued by server on register/renew)
        let turn_cred = self.credential_state.turn_credential().await;

        // Assemble ICE server list; TURN servers use TurnCredential, STUN servers use static config
        let ice_servers: Vec<RTCIceServer> = self
            .config
            .ice_servers
            .iter()
            .map(|server| {
                let is_turn = server
                    .urls
                    .iter()
                    .any(|url| url.starts_with("turn:") || url.starts_with("turns:"));

                if is_turn {
                    match &turn_cred {
                        Some(tc) => RTCIceServer {
                            urls: server.urls.clone(),
                            username: tc.username.clone(),
                            credential: tc.password.clone(),
                        },
                        None => {
                            // Credential not ready yet, skip this TURN server (missing credentials cause ICE failure,
                            // keep URL so webrtc-rs error logs show the specific address)
                            tracing::warn!(
                                "TurnCredential not ready, TURN server {} will fail authentication",
                                server.urls.first().cloned().unwrap_or_default()
                            );
                            RTCIceServer {
                                urls: server.urls.clone(),
                                username: String::new(),
                                credential: String::new(),
                            }
                        }
                    }
                } else {
                    RTCIceServer {
                        urls: server.urls.clone(),
                        username: server.username.clone().unwrap_or_default(),
                        credential: server.credential.clone().unwrap_or_default(),
                    }
                }
            })
            .collect();

        if ice_servers.is_empty() {
            tracing::info!("🌐 No ICE servers configured; proceeding without STUN/TURN servers");
        }
        tracing::info!("🌐 ICE servers configured: {:?}", ice_servers);
        // Convert ICE transport policy
        let ice_transport_policy = match self.config.ice_transport_policy {
            IceTransportPolicy::All => RTCIceTransportPolicy::All,
            IceTransportPolicy::Relay => RTCIceTransportPolicy::Relay,
        };

        // Create WebRTC configuration
        let rtc_config = RTCConfiguration {
            ice_servers,
            ice_transport_policy,
            ..Default::default()
        };

        // Create SettingEngine with role-based configuration
        let mut setting_engine = webrtc::api::setting_engine::SettingEngine::default();

        // Inject VNet if configured (test-utils only)
        #[cfg(feature = "test-utils")]
        if let Some(ref vnet) = self.vnet {
            tracing::info!("🌐 Using VNet for RTCPeerConnection (test mode)");
            setting_engine.set_vnet(Some(vnet.clone()));
        }

        // Apply ICE candidate acceptance wait times (for both Offerer and Answerer)
        self.apply_ice_wait_times(&mut setting_engine);

        // Apply advanced parameters (UDP ports, NAT 1:1) only for Answerer
        if is_answerer {
            tracing::info!("🎭 Applying advanced WebRTC parameters (Answerer mode)");
            self.apply_answerer_config(&mut setting_engine, remote_fixed)?;
        } else {
            tracing::info!("🎭 Using default WebRTC configuration (Offerer mode)");
        }

        // Opt-in shared ICE UDP mux (`ice_udp_mux_port`): route all ICE UDP
        // through one process-shared muxed socket instead of per-connection
        // ephemeral sockets, so connection churn cannot exhaust the fd table.
        // NOT the default: in webrtc-ice 0.14, muxed mode skips
        // server-reflexive (srflx) candidate gathering entirely (host and
        // TURN relay candidates only), so it is only suitable for nodes that
        // are directly reachable or have TURN configured; NAT-ed nodes
        // relying on STUN must keep the default ephemeral sockets. Under
        // VNet (tests) the virtual transport is used instead, so the mux is
        // skipped.
        if let Some(mux_port) = self.config.advanced.ice_udp_mux_port {
            #[cfg(feature = "test-utils")]
            let use_shared_mux = self.vnet.is_none();
            #[cfg(not(feature = "test-utils"))]
            let use_shared_mux = true;
            if use_shared_mux {
                use webrtc::ice::udp_network::UDPNetwork;
                setting_engine
                    .set_udp_network(UDPNetwork::Muxed(self.shared_udp_mux(mux_port).await?));
            }
        }

        // Create API with MediaEngine and SettingEngine
        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_setting_engine(setting_engine)
            .build();

        // Create PeerConnection
        let peer_connection = api.new_peer_connection(rtc_config).await?;

        peer_connection.on_ice_connection_state_change(Box::new(move |state| {
            tracing::info!("🔄 ICE Connection State Changed: {:?}", state);
            Box::pin(async move {})
        }));

        peer_connection.on_ice_gathering_state_change(Box::new(move |state| {
            tracing::info!("🔄 ICE Gathering State Changed: {:?}", state);
            Box::pin(async move {})
        }));

        tracing::info!("✅ Create RTCPeerConnection with VP8, H264, OPUS codecs");

        Ok(peer_connection)
    }

    /// Apply ICE candidate acceptance wait times (for both Offerer and Answerer)
    fn apply_ice_wait_times(
        &self,
        setting_engine: &mut webrtc::api::setting_engine::SettingEngine,
    ) {
        use std::time::Duration;
        use webrtc::ice::network_type::NetworkType;

        let advanced = &self.config.advanced;

        // Limit local ICE gathering to IPv4 UDP candidates. On local/dev setups
        // with localhost STUN/TURN, unusable UDP6 probes can stall gathering and
        // make DataChannel readiness flaky.
        setting_engine.set_network_types(vec![NetworkType::Udp4]);

        setting_engine.set_host_acceptance_min_wait(Some(Duration::from_millis(
            advanced.ice_host_acceptance_min_wait,
        )));
        setting_engine.set_srflx_acceptance_min_wait(Some(Duration::from_millis(
            advanced.ice_srflx_acceptance_min_wait,
        )));
        setting_engine.set_prflx_acceptance_min_wait(Some(Duration::from_millis(
            advanced.ice_prflx_acceptance_min_wait,
        )));
        setting_engine.set_relay_acceptance_min_wait(Some(Duration::from_millis(
            advanced.ice_relay_acceptance_min_wait,
        )));

        tracing::info!(
            "🔧 ICE wait times: host={}ms, srflx={}ms, prflx={}ms, relay={}ms",
            advanced.ice_host_acceptance_min_wait,
            advanced.ice_srflx_acceptance_min_wait,
            advanced.ice_prflx_acceptance_min_wait,
            advanced.ice_relay_acceptance_min_wait
        );
    }

    /// Apply Answerer-specific configuration (UDP ports, NAT 1:1 mapping)
    fn apply_answerer_config(
        &self,
        setting_engine: &mut webrtc::api::setting_engine::SettingEngine,
        remote_fixed: bool,
    ) -> NetworkResult<()> {
        use webrtc::ice::udp_network::{EphemeralUDP, UDPNetwork};
        use webrtc::ice_transport::ice_candidate_type::RTCIceCandidateType;

        let advanced = &self.config.advanced;

        // Determine if local has fixed configuration
        let local_fixed = advanced.udp_ports.is_some() && !advanced.public_ips.is_empty();

        // Apply UDP port strategy. When the shared ICE UDP mux is enabled
        // (ice_udp_mux_port), it takes precedence: create_peer_connection
        // overrides the UDP network with the muxed socket for every role, and
        // configuring an ephemeral port range here would be dead config.
        if let Some((min, max)) = advanced.udp_ports {
            if advanced.ice_udp_mux_port.is_some() {
                tracing::warn!(
                    "🔧 UDP port range {}-{} ignored: ice_udp_mux_port is set and \
                     the shared ICE UDP mux takes precedence",
                    min,
                    max
                );
            } else {
                let ephemeral = EphemeralUDP::new(min, max).map_err(|e| {
                    crate::transport::NetworkError::Other(anyhow::anyhow!(
                        "Failed to create EphemeralUDP: {}",
                        e
                    ))
                })?;
                setting_engine.set_udp_network(UDPNetwork::Ephemeral(ephemeral));
                tracing::info!("🔧 UDP port range: {}-{}", min, max);
            }
        } else {
            tracing::info!("🔧 Using default random UDP ports");
        }

        // Apply NAT 1:1 mapping based on local and remote configuration
        if !advanced.public_ips.is_empty() {
            match (local_fixed, remote_fixed) {
                (true, true) => {
                    // Both sides configured → Srflx only
                    setting_engine
                        .set_nat_1to1_ips(advanced.public_ips.clone(), RTCIceCandidateType::Srflx);
                    tracing::info!(
                        "🔧 NAT 1:1 IPs (Srflx only): {:?} [local_fixed={}, remote_fixed={}]",
                        advanced.public_ips,
                        local_fixed,
                        remote_fixed
                    );
                }
                (true, false) => {
                    // Only local configured → Host only
                    setting_engine
                        .set_nat_1to1_ips(advanced.public_ips.clone(), RTCIceCandidateType::Host);
                    tracing::info!(
                        "🔧 NAT 1:1 IPs (Host only): {:?} [local_fixed={}, remote_fixed={}]",
                        advanced.public_ips,
                        local_fixed,
                        remote_fixed
                    );
                }
                (false, _) => {
                    // Local not configured → Host + Srflx (default behavior)
                    setting_engine
                        .set_nat_1to1_ips(advanced.public_ips.clone(), RTCIceCandidateType::Host);
                    setting_engine
                        .set_nat_1to1_ips(advanced.public_ips.clone(), RTCIceCandidateType::Srflx);
                    tracing::info!(
                        "🔧 NAT 1:1 IPs (Host + Srflx): {:?} [local_fixed={}, remote_fixed={}]",
                        advanced.public_ips,
                        local_fixed,
                        remote_fixed
                    );
                }
            }
        }

        Ok(())
    }

    /// Create Offer (Trickle ICE mode)
    ///
    /// # Arguments
    /// - `peer_connection`: PeerConnection
    ///
    /// # Returns
    /// Offer SDP string (ICE candidates sent separately via on_ice_candidate callback)
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip_all)
    )]
    pub async fn create_offer(&self, peer_connection: &RTCPeerConnection) -> NetworkResult<String> {
        // Note: Negotiated DataChannel should be created BEFORE calling this method
        // to trigger ICE gathering (done in coordinator.rs)

        // Create Offer
        let offer = peer_connection.create_offer(None).await?;
        let offer_sdp = offer.sdp.clone();

        // Set local Description (this triggers ICE gathering)
        peer_connection.set_local_description(offer).await?;

        // DO NOT wait for ICE gathering - this is Trickle ICE
        // ICE candidates will be sent via on_ice_candidate callback

        tracing::info!(
            "✅ Create Offer (SDP length: {}, Trickle ICE mode)",
            offer_sdp.len()
        );

        Ok(offer_sdp)
    }

    /// Create ICE restart Offer (offerer side)
    pub async fn create_ice_restart_offer(
        &self,
        peer_connection: &RTCPeerConnection,
    ) -> NetworkResult<String> {
        use webrtc::peer_connection::offer_answer_options::RTCOfferOptions;

        let offer = peer_connection
            .create_offer(Some(RTCOfferOptions {
                ice_restart: true,
                ..Default::default()
            }))
            .await?;
        let offer_sdp = offer.sdp.clone();

        peer_connection.set_local_description(offer).await?;

        tracing::info!(
            "✅ Create ICE Restart Offer (SDP length: {}, Trickle ICE mode)",
            offer_sdp.len()
        );

        Ok(offer_sdp)
    }

    /// Handle Answer
    ///
    /// # Arguments
    /// - `peer_connection`: PeerConnection
    /// - `answer_sdp`: Answer SDP string
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip_all, fields(answer_len = answer_sdp.len()))
    )]
    pub async fn handle_answer(
        &self,
        peer_connection: &RTCPeerConnection,
        answer_sdp: String,
    ) -> NetworkResult<()> {
        // Setremote Description
        let answer = RTCSessionDescription::answer(answer_sdp)?;
        peer_connection.set_remote_description(answer).await?;

        tracing::info!("✅ Handle Answer");

        Ok(())
    }

    /// Create Answer (passive side, Trickle ICE mode)
    ///
    /// # Arguments
    /// - `peer_connection`: PeerConnection
    /// - `offer_sdp`: Offer SDP string
    ///
    /// # Returns
    /// Answer SDP string (ICE candidates sent separately)
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(level = "info", skip_all)
    )]
    pub async fn create_answer(
        &self,
        peer_connection: &RTCPeerConnection,
        offer_sdp: String,
    ) -> NetworkResult<String> {
        // Set remote Description（Offer）
        let offer = RTCSessionDescription::offer(offer_sdp)?;
        peer_connection.set_remote_description(offer).await?;

        // Create Answer
        let answer = peer_connection.create_answer(None).await?;
        let answer_sdp = answer.sdp.clone();

        // Set local Description (triggers ICE gathering)
        peer_connection.set_local_description(answer).await?;

        // DO NOT wait for ICE gathering - Trickle ICE mode
        // ICE candidates will be sent via on_ice_candidate callback

        tracing::info!(
            "✅ Create Answer (SDP length: {}, Trickle ICE mode)",
            answer_sdp.len()
        );

        Ok(answer_sdp)
    }

    /// add ICE Candidate
    ///
    /// # Arguments
    /// - `peer_connection`: PeerConnection
    /// - `candidate`: ICE Candidate and its ICE-generation metadata
    #[cfg_attr(
        feature = "opentelemetry",
        tracing::instrument(
            level = "trace",
            skip_all,
            fields(candidate_len = candidate.candidate.len())
        )
    )]
    pub async fn add_ice_candidate(
        &self,
        peer_connection: &RTCPeerConnection,
        candidate: IceCandidate,
    ) -> NetworkResult<()> {
        use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

        let sdp_mline_index = candidate
            .sdp_mline_index
            .map(u16::try_from)
            .transpose()
            .map_err(|_| {
                crate::transport::NetworkError::InvalidArgument(
                    "ICE candidate sdp_mline_index exceeds u16".to_string(),
                )
            })?;
        let ice_candidate = RTCIceCandidateInit {
            candidate: candidate.candidate,
            sdp_mid: candidate.sdp_mid,
            sdp_mline_index,
            username_fragment: candidate.username_fragment,
        };

        peer_connection.add_ice_candidate(ice_candidate).await?;

        tracing::trace!("✅ add ICE Candidate");

        Ok(())
    }
}

#[cfg(test)]
#[path = "negotiator_tests.rs"]
mod tests;
