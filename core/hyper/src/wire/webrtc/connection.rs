//! WebRTC P2P Connection implementation

use crate::transport::lane::classify_peer_connection_error;
use crate::transport::session::ConnectionSession;
use crate::transport::{
    ConnType, DataLane, NetworkError, NetworkResult, WebRtcDataLane, WireHandle,
};
use crate::transport::{ConnectionEvent, ConnectionState};
use actr_protocol::prost::Message;
use actr_protocol::{ActrId, PayloadType};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, broadcast, mpsc};
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::{RTCPeerConnection, peer_connection_state::RTCPeerConnectionState};
use webrtc::rtp_transceiver::rtp_sender::RTCRtpSender;
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;

/// Type alias for media track storage (track_id → (Track, Sender))
type MediaTracks = Arc<RwLock<HashMap<String, (Arc<TrackLocalStaticRTP>, Arc<RTCRtpSender>)>>>;

/// Type alias for lane cache array (PayloadType index → cached DataLane)
type LaneCache<const N: usize> = Arc<RwLock<[Option<Arc<dyn DataLane>>; N]>>;

const PEER_CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_millis(500);

/// WebRtcConnection - WebRTC P2P Connect
#[derive(Clone)]
pub(crate) struct WebRtcConnection {
    /// Peer ID for event identification
    peer_id: ActrId,

    /// underlying RTCPeerConnection
    peer_connection: Arc<RTCPeerConnection>,

    // TODO: useless property, remove this
    /// DataChannel Cache：PayloadType → DataChannel（4 types use DataChannel）
    /// index reference mapping：RpcReliable(0), RpcSignal(1), StreamReliable(2), StreamLatencyFirst(3)
    data_channels: Arc<RwLock<[Option<Arc<RTCDataChannel>>; 4]>>,

    /// MediaTrack Cache：track_id → (Track, RtpSender)
    media_tracks: MediaTracks,

    /// RTP sequence numbers per track (track_id → sequence_number)
    track_sequence_numbers: Arc<RwLock<HashMap<String, Arc<AtomicU16>>>>,

    /// RTP SSRC per track (track_id → ssrc)
    track_ssrcs: Arc<RwLock<HashMap<String, u32>>>,

    /// Lane Cache: PayloadType -> Lane (4 types use DataChannel)
    /// index mapping: RpcReliable(0), RpcSignal(1), StreamReliable(2), StreamLatencyFirst(3)
    /// MediaTrack not cached in array, uses HashMap
    lane_cache: LaneCache<4>,

    /// Event broadcaster for connection state changes
    event_tx: broadcast::Sender<ConnectionEvent>,

    /// Connection session (session_id + cancel_token + close-once)
    session: ConnectionSession,

    /// connection status (legacy, will be replaced by session.is_closed())
    connected: Arc<RwLock<bool>>,
}

impl std::fmt::Debug for WebRtcConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRtcConnection")
            .field("peer_id", &self.peer_id)
            .field("peer_connection", &"<RTCPeerConnection>")
            .field("data_channels", &"<[Option<Arc<RTCDataChannel>>; 4]>")
            .field("media_tracks", &"<HashMap<String, Arc<Track>>>")
            .field("connected", &self.connected)
            .finish()
    }
}

impl WebRtcConnection {
    /// Create WebRtcConnection from RTCPeerConnection
    ///
    /// # Arguments
    /// - `peer_id`: Peer identity for event identification
    /// - `peer_connection`: Arc wrapped RTCPeerConnection
    /// - `event_tx`: Broadcast sender for connection events
    pub fn new(
        peer_id: ActrId,
        peer_connection: Arc<RTCPeerConnection>,
        event_tx: broadcast::Sender<ConnectionEvent>,
    ) -> Self {
        Self {
            peer_id,
            peer_connection,
            data_channels: Arc::new(RwLock::new([None, None, None, None])),
            media_tracks: Arc::new(RwLock::new(HashMap::new())),
            track_sequence_numbers: Arc::new(RwLock::new(HashMap::new())),
            track_ssrcs: Arc::new(RwLock::new(HashMap::new())),
            lane_cache: Arc::new(RwLock::new([None, None, None, None])),
            event_tx,
            session: ConnectionSession::new(),
            connected: Arc::new(RwLock::new(true)),
        }
    }

    /// Get session ID
    pub(crate) fn session_id(&self) -> u64 {
        self.session.session_id
    }

    /// Install a state-change handler on the underlying RTCPeerConnection.
    ///
    /// This keeps `connected` in sync with the WebRTC connection state and
    /// broadcasts state change events for upper layers to handle.
    pub(crate) async fn handle_state_change(&self, state: RTCPeerConnectionState) {
        if self.session.is_cancelled() {
            // Preserve the terminal state event for session-guarded cleanup
            // observers, but skip hooks and recursive close side effects.
            if matches!(state, RTCPeerConnectionState::Closed) {
                let _ = self.event_tx.send(ConnectionEvent::StateChanged {
                    peer_id: self.peer_id.clone(),
                    session_id: self.session.session_id,
                    state: ConnectionState::Closed,
                });
            }
            tracing::debug!(
                "🚫 handle_state_change session {} cancelled, ignoring {:?}",
                self.session.session_id,
                state
            );
            return;
        }

        // Treat New/Connecting/Connected as "connected"; others as disconnected.
        let is_connected = matches!(
            state,
            RTCPeerConnectionState::New
                | RTCPeerConnectionState::Connecting
                | RTCPeerConnectionState::Connected
        );

        // Update flag and detect transitions from connected -> disconnected.
        let was_connected = {
            let mut flag = self.connected.write().await;
            let prev = *flag;
            *flag = is_connected;
            prev
        };

        // Convert WebRTC state to our ConnectionState
        let connection_state = match state {
            RTCPeerConnectionState::New => ConnectionState::New,
            RTCPeerConnectionState::Connecting => ConnectionState::Connecting,
            RTCPeerConnectionState::Connected => ConnectionState::Connected,
            RTCPeerConnectionState::Disconnected => ConnectionState::Disconnected,
            RTCPeerConnectionState::Failed => ConnectionState::Failed,
            RTCPeerConnectionState::Closed => ConnectionState::Closed,
            _ => ConnectionState::Closed, // Unspecified maps to Closed
        };

        tracing::info!(
            "🔄 WebRtcConnection peer state changed: {:?}, connected={}",
            state,
            is_connected
        );

        // Broadcast state change event for upper layers
        let _ = self.event_tx.send(ConnectionEvent::StateChanged {
            peer_id: self.peer_id.clone(),
            session_id: self.session.session_id,
            state: connection_state.clone(),
        });

        // For Closed state, proactively close the connection and let
        // `close()` perform all resource cleanup. Only trigger when we
        // transition from connected -> disconnected to avoid loops.
        if was_connected && matches!(state, RTCPeerConnectionState::Closed) {
            tracing::info!(
                "🔻 WebRtcConnection entering terminal state {:?}, calling close()",
                state
            );

            if let Err(e) = self.close().await {
                tracing::warn!("⚠️ WebRtcConnection::close() failed: {}", e);
            }
        }
    }

    /// Mark the connection as connected.
    ///
    /// The underlying WebRTC connection has already been established via
    /// signaling; this call only records the local "connected" flag.
    pub(crate) async fn connect(&self) -> NetworkResult<()> {
        *self.connected.write().await = true;
        Ok(())
    }

    /// Broadcast DataChannel closed event
    ///
    /// Unlike the old AtomicBool-based notification, this broadcasts to all
    /// subscribers every time a DataChannel closes.
    fn notify_data_channel_closed(&self, payload_type: PayloadType) {
        //
        // The cleanup will be handled by the caller (close() or cleanup_cancelled_connection).
        // We only broadcast the event here to notify upper layers.
        let _ = self.event_tx.send(ConnectionEvent::DataChannelClosed {
            peer_id: self.peer_id.clone(),
            session_id: self.session.session_id,
            payload_type,
        });
    }

    /// Return a snapshot of the current DataChannel cache.
    ///
    /// Used by the coordinator to query `buffered_amount` on abnormal disconnect.
    pub async fn data_channels(&self) -> [Option<Arc<RTCDataChannel>>; 4] {
        self.data_channels.read().await.clone()
    }

    /// Check if any DataChannel is open
    pub async fn has_open_data_channel(&self) -> bool {
        use webrtc::data_channel::data_channel_state::RTCDataChannelState;

        let channels = self.data_channels.read().await;
        for channel in channels.iter().flatten() {
            if channel.ready_state() == RTCDataChannelState::Open {
                return true;
            }
        }
        false
    }

    /// Drain all open DataChannel send buffers before closing (graceful shutdown).
    ///
    /// Polls `buffered_amount()` up to 50 times with 100 ms intervals (max 5 s total).
    /// Logs a warning if the buffer is still non-zero when the timeout expires.
    async fn drain_data_channels(&self) {
        use webrtc::data_channel::data_channel_state::RTCDataChannelState;
        const MAX_POLLS: u32 = 50;
        const POLL_INTERVAL_MS: u64 = 100;

        // Snapshot open channels first, then release the lock before any async waits.
        let open_channels: Vec<(usize, Arc<RTCDataChannel>)> = {
            let channels = self.data_channels.read().await;
            channels
                .iter()
                .enumerate()
                .filter_map(|(idx, opt)| {
                    opt.as_ref().and_then(|ch| {
                        if ch.ready_state() == RTCDataChannelState::Open {
                            Some((idx, Arc::clone(ch)))
                        } else {
                            None
                        }
                    })
                })
                .collect()
        };

        for (idx, channel) in open_channels {
            let label = channel.label().to_owned();
            for attempt in 0..MAX_POLLS {
                let buffered = channel.buffered_amount().await;
                if buffered == 0 {
                    if attempt > 0 {
                        tracing::debug!(
                            peer_id = %self.peer_id,
                            channel = %label,
                            channel_idx = idx,
                            attempts = attempt,
                            "DataChannel send buffer drained",
                        );
                    }
                    break;
                }

                if attempt == MAX_POLLS - 1 {
                    tracing::warn!(
                        peer_id = %self.peer_id,
                        channel = %label,
                        channel_idx = idx,
                        buffered_bytes = buffered,
                        "DataChannel send buffer not fully drained before close; \
                         data may be lost for the peer",
                    );
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
                }
            }
        }
    }

    /// Close connection and broadcast ConnectionClosed event
    ///
    /// This method is idempotent: only the first call performs the actual close.
    /// Subsequent calls return Ok(()) immediately.
    pub async fn close(&self) -> NetworkResult<()> {
        // Idempotent: only execute once per session
        if !self.session.try_close() {
            tracing::debug!(
                "🔒 [close] serial={} already closed (session_id={}), skipping",
                self.peer_id,
                self.session.session_id
            );
            return Ok(());
        }

        // Cancel the session token — all callbacks holding a clone will notice
        self.session.cancel();

        tracing::debug!(
            "🔒 [close] serial={} session_id={} step 1: marking closed",
            self.peer_id,
            self.session.session_id
        );
        *self.connected.write().await = false;

        // Drain DataChannel send buffers before closing (graceful shutdown).
        self.drain_data_channels().await;

        // Notify upper layers before awaiting RTCPeerConnection::close().
        // Mobile background/resume paths can stall inside the lower-level close
        // after SCTP/ICE has already become unusable; send recovery must not
        // depend on that await completing.
        let _ = self.event_tx.send(ConnectionEvent::ConnectionClosed {
            peer_id: self.peer_id.clone(),
            session_id: self.session.session_id,
        });

        tracing::debug!(
            "🔒 [close] serial={} step 2: closing peer_connection",
            self.peer_id
        );
        let close_result =
            tokio::time::timeout(PEER_CONNECTION_CLOSE_TIMEOUT, self.peer_connection.close()).await;
        let close_error = match close_result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => {
                tracing::warn!(
                    peer_id = %self.peer_id,
                    session_id = self.session.session_id,
                    "RTCPeerConnection close timed out",
                );
                None
            }
        };

        // Break the RTCPeerConnection <-> handler reference cycle so the peer
        // connection can actually be dropped after cleanup. The state-change and
        // data-channel handlers installed on the peer connection each capture a
        // strong `WebRtcConnection` clone, which itself owns an
        // `Arc<RTCPeerConnection>` -- so the connection keeps itself alive through
        // its own callbacks. `RTCPeerConnection::close()` closes the ICE transport
        // but does NOT clear these handlers, and in webrtc 0.14 the ICE agent's
        // per-interface UDP sockets are released on drop, not on close(). Without
        // clearing the handlers the connection never drops, so those sockets leak
        // for the life of the process and eventually exhaust the fd limit.
        // Replacing the handlers with no-ops drops the captured clones and breaks
        // the cycle, so releasing the last `Arc<RTCPeerConnection>` frees the
        // sockets.
        self.peer_connection
            .on_peer_connection_state_change(Box::new(move |_state| Box::pin(async move {})));
        self.peer_connection
            .on_data_channel(Box::new(move |_dc| Box::pin(async move {})));

        // Clear each cache under a dedicated lock scope
        {
            let mut cache = self.lane_cache.write().await;
            *cache = [None, None, None, None];
        }
        {
            let mut channels = self.data_channels.write().await;
            *channels = [None, None, None, None];
        }
        {
            let mut tracks = self.media_tracks.write().await;
            tracks.clear();
        }
        {
            let mut seq_nums = self.track_sequence_numbers.write().await;
            seq_nums.clear();
        }
        {
            let mut ssrcs = self.track_ssrcs.write().await;
            ssrcs.clear();
        }

        tracing::info!(
            "🔌 WebRtcConnection closed for peer {:?} (session_id={})",
            self.peer_id,
            self.session.session_id
        );

        if let Some(error) = close_error {
            return Err(error.into());
        }

        Ok(())
    }

    /// based on PayloadType configuration DataChannel
    fn get_data_channel_config(
        payload_type: &PayloadType,
    ) -> webrtc::data_channel::data_channel_init::RTCDataChannelInit {
        use webrtc::data_channel::data_channel_init::RTCDataChannelInit;

        match payload_type {
            PayloadType::StreamLatencyFirst => {
                // partial reliable transmission (low latency priority)
                RTCDataChannelInit {
                    ordered: Some(false),
                    max_retransmits: Some(3),
                    max_packet_life_time: None,
                    protocol: Some("".to_string()),
                    negotiated: None,
                }
            }
            _ => {
                // default reliable transmission
                RTCDataChannelInit {
                    ordered: Some(true),
                    max_retransmits: None,
                    max_packet_life_time: None,
                    protocol: Some("".to_string()),
                    negotiated: None,
                }
            }
        }
    }
}

impl WebRtcConnection {
    /// Get or create DataLane (with caching)
    pub async fn get_lane(&self, payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        self.get_lane_internal(payload_type).await
    }

    /// Internal implementation of get_lane
    async fn get_lane_internal(
        &self,
        payload_type: PayloadType,
    ) -> NetworkResult<Arc<dyn DataLane>> {
        // MediaTrack not supported in this method (needs stream_id)
        if payload_type == PayloadType::MediaRtp {
            return Err(NetworkError::NotImplemented(
                "MediaTrack Lane requires stream_id, use get_media_lane() instead".to_string(),
            ));
        }

        let idx = payload_type as usize;

        // 1. Check cache
        let mut need_recreate = false;
        {
            let cache = self.lane_cache.read().await;
            if let Some(lane) = &cache[idx] {
                // Check if the cached lane's transport is still healthy
                if !lane.is_healthy() {
                    tracing::warn!(
                        "Cached lane for {:?} is unhealthy, recreating",
                        payload_type,
                    );
                    need_recreate = true;
                } else {
                    tracing::debug!("Reuse cached DataLane: {:?}", payload_type);
                    return Ok(Arc::clone(lane));
                }
            }
        }

        if need_recreate {
            // Clear stale cache entries before recreating.
            let mut cache = self.lane_cache.write().await;
            cache[idx] = None;
            let mut channels = self.data_channels.write().await;
            channels[idx] = None;
        }

        // 2. Create new DataLane
        let lane = self.create_lane_internal(payload_type).await?;

        // 3. Cache
        {
            let mut cache = self.lane_cache.write().await;
            cache[idx] = Some(Arc::clone(&lane));
        }

        tracing::info!("✨ WebRtcConnection Createnew DataLane: {:?}", payload_type);

        Ok(lane)
    }

    /// Invalidate cached lane/DataChannel for given payload type.
    ///
    /// Used when the underlying DataChannel has transitioned to Closed and needs
    /// to be recreated on next `get_lane` call.
    pub async fn invalidate_lane(&self, payload_type: PayloadType) {
        self.invalidate_lane_internal(payload_type).await;
    }

    /// Internal implementation of invalidate_lane
    async fn invalidate_lane_internal(&self, payload_type: PayloadType) {
        let idx = payload_type as usize;
        let mut cache = self.lane_cache.write().await;
        cache[idx] = None;
        let mut channels = self.data_channels.write().await;
        channels[idx] = None;
    }

    /// Internal: Create DataChannel Lane (without cache)
    async fn create_lane_internal(
        &self,
        payload_type: PayloadType,
    ) -> NetworkResult<Arc<dyn DataLane>> {
        // Media tracks use a different code path
        if payload_type == PayloadType::MediaRtp {
            return Err(NetworkError::NotImplemented(
                "MediaTrack Lane not implemented in this method".to_string(),
            ));
        }

        // Create new DataChannel
        let mut channels = self.data_channels.write().await;

        let label = payload_type.as_str_name();

        let dc_config = Self::get_data_channel_config(&payload_type);
        let data_channel = self
            .peer_connection
            .create_data_channel(label, Some(dc_config))
            .await
            .map_err(|e| {
                classify_peer_connection_error(e, self.peer_connection.connection_state())
            })?;

        // Register on_open callback to send DataChannelOpened event
        let event_tx_for_open = self.event_tx.clone();
        let peer_id_for_open = self.peer_id.clone();
        let session_id_for_open = self.session.session_id;
        let payload_type_for_open = payload_type;

        data_channel.on_open(Box::new(move || {
            let event_tx = event_tx_for_open.clone();
            let peer_id = peer_id_for_open.clone();
            let payload_type = payload_type_for_open;

            tracing::info!("🔄 WebRTC DataChannel opened: {:?}", payload_type);

            Box::pin(async move {
                let _ = event_tx.send(ConnectionEvent::DataChannelOpened {
                    peer_id,
                    session_id: session_id_for_open,
                    payload_type,
                });
                tracing::debug!("📣 DataChannelOpened event sent for {:?}", payload_type);
            })
        }));

        let channel_id = data_channel.id();
        let payload_type_for_error = payload_type;
        let label_for_error = label;
        data_channel.on_error(Box::new(move |error| {
            let payload_type = payload_type_for_error;
            let label = label_for_error;
            let channel_id = channel_id;
            tracing::warn!(
                "⚠️ WebRTC DataChannel error [{}] (payload_type={:?}, channel_id={}): {:?}",
                label,
                payload_type,
                channel_id,
                error
            );
            Box::pin(async move {})
        }));

        let session_for_close = self.session.clone();
        let lane_cache_for_close = self.lane_cache.clone();
        let data_channels_for_close = self.data_channels.clone();
        let event_tx_for_close = self.event_tx.clone();
        let peer_id_for_close = self.peer_id.clone();
        let sid_for_close = self.session.session_id;
        let payload_type_for_close = payload_type;
        let label_for_close = label;
        let channel_id_for_close = channel_id;
        let dc_for_close = Arc::clone(&data_channel);
        data_channel.on_close(Box::new(move || {
            let session = session_for_close.clone();
            let lane_cache = lane_cache_for_close.clone();
            let data_channels = data_channels_for_close.clone();
            let event_tx = event_tx_for_close.clone();
            let peer_id = peer_id_for_close.clone();
            let payload_type = payload_type_for_close;
            let label = label_for_close;
            let channel_id = channel_id_for_close;
            let dc = dc_for_close.clone();
            Box::pin(async move {
                // Guard: if session is cancelled (connection already cleaned up),
                // skip all side effects to avoid corrupting a new connection
                if session.is_cancelled() {
                    tracing::debug!(
                        "🚫 DC.on_close session {} cancelled, ignoring for {:?}",
                        sid_for_close,
                        payload_type
                    );
                    return;
                }

                // Query buffered_amount at the moment of close to surface potential data loss.
                let buffered = dc.buffered_amount().await;
                if buffered > 0 {
                    tracing::warn!(
                        channel = %label,
                        channel_id = channel_id,
                        payload_type = ?payload_type,
                        buffered_bytes = buffered,
                        "DataChannel closed with non-empty send buffer",
                    );
                } else {
                    tracing::warn!(
                        "DataChannel closed [{}] (payload_type={:?}, channel_id={})",
                        label,
                        payload_type,
                        channel_id,
                    );
                }
                // Invalidate cached lane when DataChannel closes
                let idx = payload_type as usize;
                {
                    let mut cache = lane_cache.write().await;
                    cache[idx] = None;
                }
                {
                    let mut channels = data_channels.write().await;
                    channels[idx] = None;
                }
                // Broadcast DataChannelClosed event
                let _ = event_tx.send(ConnectionEvent::DataChannelClosed {
                    peer_id,
                    session_id: sid_for_close,
                    payload_type,
                });
            })
        }));

        // CreateReceive channel （using Bytes）
        let (tx, rx) = mpsc::channel(100);

        // Set onmessage return adjust
        let tx_clone = tx.clone();
        data_channel.on_message(Box::new(
            move |msg: webrtc::data_channel::data_channel_message::DataChannelMessage| {
                // zero-copy： directly using msg.data (Bytes)
                let data = msg.data;
                tracing::debug!("🔄 WebRTC DataChannel message received1111: {:?}", data);
                let tx = tx_clone.clone();
                Box::pin(async move {
                    if let Err(e) = tx.send(data).await {
                        tracing::warn!("❌ WebRTC DataChannel messageSend to Lane failure: {}", e);
                    }
                })
            },
        ));

        // Cache DataChannel（ index reference directly using PayloadType value ）
        let idx = payload_type as usize;
        channels[idx] = Some(Arc::clone(&data_channel));

        // Returns Lane
        Ok(Arc::new(WebRtcDataLane::new(data_channel, rx)))
    }

    /// Add media track to PeerConnection
    ///
    /// # Arguments
    /// - `track_id`: Unique track identifier
    /// - `codec`: Codec name (e.g., "H264", "VP8", "opus")
    /// - `media_type`: "video" or "audio"
    ///
    /// # Returns
    /// Reference to the created TrackLocalStaticRTP
    ///
    /// # Note
    /// Must be called BEFORE create_offer/create_answer for track to appear in SDP
    pub async fn add_media_track(
        &self,
        track_id: String,
        codec: &str,
        media_type: &str,
    ) -> NetworkResult<Arc<TrackLocalStaticRTP>> {
        use webrtc::api::media_engine::MIME_TYPE_H264;
        use webrtc::api::media_engine::MIME_TYPE_OPUS;
        use webrtc::api::media_engine::MIME_TYPE_VP8;
        use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

        // Reuse existing track so repeated start/stop flows can safely retry.
        if let Some((track, _sender)) = self.media_tracks.read().await.get(&track_id).cloned() {
            tracing::info!("♻️ Reusing existing media track: {}", track_id);
            return Ok(track);
        }

        // Determine MIME type based on codec and media_type
        let mime_type = match (media_type, codec.to_uppercase().as_str()) {
            ("video", "H264") => MIME_TYPE_H264,
            ("video", "VP8") => MIME_TYPE_VP8,
            ("audio", "OPUS") => MIME_TYPE_OPUS,
            _ => {
                return Err(NetworkError::WebRtcError(format!(
                    "Unsupported codec: {codec} for {media_type}"
                )));
            }
        };

        // Create TrackLocalStaticRTP
        let track = Arc::new(TrackLocalStaticRTP::new(
            RTCRtpCodecCapability {
                mime_type: mime_type.to_string(),
                ..Default::default()
            },
            track_id.clone(),
            format!("actr-{media_type}"), // stream_id
        ));

        // Add track to PeerConnection
        let rtp_sender =
            self.peer_connection
                .add_track(Arc::clone(&track)
                    as Arc<dyn webrtc::track::track_local::TrackLocal + Send + Sync>)
                .await?;

        // Cache track and sender
        let mut tracks = self.media_tracks.write().await;
        tracks.insert(track_id.clone(), (Arc::clone(&track), rtp_sender));

        // Initialize sequence number for this track
        let mut seq_nums = self.track_sequence_numbers.write().await;
        seq_nums.insert(track_id.clone(), Arc::new(AtomicU16::new(0)));

        // Generate unique SSRC for this track (random u32)
        let ssrc = rand::random::<u32>();
        let mut ssrcs = self.track_ssrcs.write().await;
        ssrcs.insert(track_id.clone(), ssrc);

        tracing::info!(
            "✨ Added media track: id={}, codec={}, type={}, ssrc=0x{:08x}",
            track_id,
            codec,
            media_type,
            ssrc
        );

        Ok(track)
    }

    /// Remove a media track and its RTP sender from the PeerConnection
    pub async fn remove_media_track(&self, track_id: &str) -> NetworkResult<()> {
        let removed = self.media_tracks.write().await.remove(track_id);
        if let Some((_track, rtp_sender)) = removed {
            self.peer_connection.remove_track(&rtp_sender).await?;
            self.track_sequence_numbers.write().await.remove(track_id);
            self.track_ssrcs.write().await.remove(track_id);
            tracing::info!("🗑️ Removed media track: {}", track_id);
        }
        Ok(())
    }

    /// Get existing media track by ID
    pub async fn get_media_track(&self, track_id: &str) -> Option<Arc<TrackLocalStaticRTP>> {
        let tracks = self.media_tracks.read().await;
        tracks
            .get(track_id)
            .map(|(track, _sender)| Arc::clone(track))
    }

    /// Get next RTP sequence number for track (atomically increments)
    ///
    /// # Arguments
    /// - `track_id`: Track identifier
    ///
    /// # Returns
    /// Next sequence number (wraps at 65535)
    pub async fn next_sequence_number(&self, track_id: &str) -> Option<u16> {
        let seq_nums = self.track_sequence_numbers.read().await;
        seq_nums
            .get(track_id)
            .map(|atomic_seq| atomic_seq.fetch_add(1, Ordering::SeqCst))
    }

    /// Get SSRC for track
    ///
    /// # Arguments
    /// - `track_id`: Track identifier
    ///
    /// # Returns
    /// SSRC value for this track
    pub async fn get_ssrc(&self, track_id: &str) -> Option<u32> {
        let ssrcs = self.track_ssrcs.read().await;
        ssrcs.get(track_id).copied()
    }

    /// Register received DataChannel (for passive side)
    ///
    /// When receiving an Offer, the passive side should register DataChannels
    /// received via on_data_channel callback instead of creating new ones.
    pub async fn register_received_data_channel(
        &self,
        data_channel: Arc<RTCDataChannel>,
        payload_type: PayloadType,
        message_tx: mpsc::UnboundedSender<(Vec<u8>, Bytes, PayloadType)>,
    ) -> NetworkResult<Arc<dyn DataLane>> {
        // Check if it's MediaTrack type
        if payload_type == PayloadType::MediaRtp {
            return Err(NetworkError::NotImplemented(
                "MediaTrack Lane not supported in this method".to_string(),
            ));
        }

        let idx = payload_type as usize;
        tracing::debug!(
            "🔄 WebRTC DataChannel registered received: {:?}, idx={}",
            payload_type,
            idx
        );
        let label = format!("{payload_type:?}");

        // Register on_open callback to send DataChannelOpened event
        let event_tx_for_open = self.event_tx.clone();
        let peer_id_for_open = self.peer_id.clone();
        let session_id_for_open = self.session.session_id;
        let payload_type_for_open = payload_type;

        data_channel.on_open(Box::new(move || {
            let event_tx = event_tx_for_open.clone();
            let peer_id = peer_id_for_open.clone();
            let payload_type = payload_type_for_open;

            tracing::info!(
                "🔄 WebRTC DataChannel opened (received): {:?}",
                payload_type
            );

            Box::pin(async move {
                let _ = event_tx.send(ConnectionEvent::DataChannelOpened {
                    peer_id,
                    session_id: session_id_for_open,
                    payload_type,
                });
                tracing::debug!("📣 DataChannelOpened event sent for {:?}", payload_type);
            })
        }));

        // Set error handler
        let payload_type_for_error = payload_type;
        let label_for_error = label.clone();
        data_channel.on_error(Box::new(move |error| {
            let payload_type = payload_type_for_error;
            let label = label_for_error.clone();
            tracing::warn!(
                "⚠️ WebRTC DataChannel error [{}] (payload_type={:?} ): {:?}",
                label,
                payload_type,
                error
            );
            Box::pin(async move {})
        }));

        // Set close handler
        let this_for_close = self.clone();
        let payload_type_for_close = payload_type;
        let label_for_close = label.clone();
        let dc_for_close = Arc::clone(&data_channel);

        data_channel.on_close(Box::new(move || {
            let this = this_for_close.clone();
            let payload_type = payload_type_for_close;
            let label = label_for_close.clone();
            let dc = dc_for_close.clone();

            Box::pin(async move {
                // Query buffered_amount at the moment of close to surface potential data loss.
                let buffered = dc.buffered_amount().await;
                if buffered > 0 {
                    tracing::warn!(
                        peer_id = %this.peer_id,
                        channel = %label,
                        payload_type = ?payload_type,
                        buffered_bytes = buffered,
                        "DataChannel (received) closed with non-empty send buffer; \
                         buffered data was likely not delivered to peer",
                    );
                } else {
                    tracing::warn!(
                        "DataChannel (received) closed [{}] (payload_type={:?})",
                        label,
                        payload_type,
                    );
                }
                // Invalidate cached lane when DataChannel closes
                this.invalidate_lane(payload_type).await;
                // Broadcast DataChannelClosed event (sync, no await needed)
                this.notify_data_channel_closed(payload_type);
            })
        }));

        // Create receive channel
        let (tx, rx) = mpsc::channel(100);

        // Set on_message callback
        let tx_clone = tx.clone();
        data_channel.on_message(Box::new(
            move |msg: webrtc::data_channel::data_channel_message::DataChannelMessage| {
                let data = msg.data;
                let tx = tx_clone.clone();
                Box::pin(async move {
                    if let Err(e) = tx.send(data).await {
                        tracing::warn!("❌ WebRTC DataChannel message send to Lane failed: {}", e);
                    }
                })
            },
        ));

        // Cache DataChannel
        {
            let mut channels = self.data_channels.write().await;
            channels[idx] = Some(Arc::clone(&data_channel));
        }

        // Create and cache Lane
        let lane: Arc<dyn DataLane> = Arc::new(WebRtcDataLane::new(data_channel, rx));
        {
            let mut cache = self.lane_cache.write().await;
            cache[idx] = Some(Arc::clone(&lane));
        }

        tracing::info!(
            "✨ WebRtcConnection registered received DataChannel: {:?}",
            payload_type
        );
        let peer_id_clone = self.peer_id.clone();
        let lane_clone = lane.clone();
        tokio::spawn(async move {
            // Continuously receive messages
            loop {
                match lane_clone.recv().await {
                    Ok(data) => {
                        tracing::debug!(
                            "📨 Received message from {:?} (PayloadType: {:?}): {} bytes",
                            peer_id_clone,
                            payload_type,
                            data.len()
                        );

                        // Serialize peer_id as bytes
                        let peer_id_bytes = peer_id_clone.encode_to_vec();

                        // Send to aggregation channel (include PayloadType)
                        if let Err(e) = message_tx.send((peer_id_bytes, data, payload_type)) {
                            tracing::error!("❌ Message aggregation failed: {:?}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "❌ Peer {:?} message receive failed (PayloadType: {:?}): {}",
                            peer_id_clone,
                            payload_type,
                            e
                        );
                        break;
                    }
                }
            }
        });

        Ok(lane)
    }
}

#[async_trait]
impl WireHandle for WebRtcConnection {
    fn connection_type(&self) -> ConnType {
        ConnType::WebRTC
    }

    fn priority(&self) -> u8 {
        1 // WebRTC has higher priority
    }

    async fn connect(&self) -> NetworkResult<()> {
        Self::connect(self).await
    }

    fn is_connected(&self) -> bool {
        !self.session.is_closed()
    }

    async fn close(&self) -> NetworkResult<()> {
        Self::close(self).await
    }

    async fn get_lane(&self, payload_type: PayloadType) -> NetworkResult<Arc<dyn DataLane>> {
        self.get_lane_internal(payload_type).await
    }

    async fn invalidate_lane(&self, payload_type: PayloadType) {
        self.invalidate_lane_internal(payload_type).await;
    }

    fn identity(&self) -> Option<crate::transport::WireIdentity> {
        Some(crate::transport::WireIdentity::WebRtc {
            peer_id: self.peer_id.clone(),
            session_id: self.session_id(),
        })
    }
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
