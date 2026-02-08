use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use iroh_gossip::api::{GossipReceiver, GossipSender};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info, trace, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    /// A single key was inserted locally, propagate immediately.
    Insert { key: String, timestamp: u64 },
    /// Full state dump.  `nonce` ensures gossip never deduplicates the message.
    State {
        entries: Vec<(String, u64)>,
        nonce: u64,
    },
}

#[derive(Debug, Clone)]
pub struct KvEvent {
    pub key: String,
    pub timestamp: u64,
    /// `true` when the key came from a remote peer.
    pub remote: bool,
}

/// Gossip-backed key store.
///
/// * **Keys are the data**, every key is a plain `String` that encodes
///   whatever information you need (paths, ids, …).
/// * **Values are timestamps**, on conflict the highest timestamp wins
///   (simple last-writer-wins).
/// * Offers `query_all` / `query_prefix` à la iroh-docs.
/// * Fully clonable, backed by `Arc<Mutex<BTreeMap>>`.
///
/// # Synchronisation strategy
///
/// | trigger              | action                                          |
/// |----------------------|-------------------------------------------------|
/// | local `insert()`     | broadcast an `Insert` message immediately       |
/// | `NeighborUp`         | schedule a full `State` broadcast (with jitter)  |
/// | periodic timer       | schedule a full `State` broadcast (with jitter)  |
/// | received `Insert`    | apply if newer, notify subscribers               |
/// | received `State`     | merge all entries (latest-ts wins), notify       |
#[derive(Debug, Clone)]
pub struct Kv {
    inner: Arc<KvInner>,
}

#[derive(Debug)]
struct KvInner {
    store: Mutex<BTreeMap<String, u64>>,
    sender: GossipSender,
    updates: broadcast::Sender<KvEvent>,
}
impl Kv {
    /// Spawn the background gossip worker and return a clonable handle.
    ///
    /// `receiver` is consumed, the worker exclusively owns the gossip
    /// receive side.
    pub fn spawn(sender: GossipSender, receiver: GossipReceiver) -> Self {
        let (updates_tx, _) = broadcast::channel(512);

        let inner = Arc::new(KvInner {
            store: Mutex::new(BTreeMap::new()),
            sender,
            updates: updates_tx,
        });

        let kv = Kv {
            inner: inner.clone(),
        };

        let kv_worker = kv.clone();
        tokio::spawn(async move {
            worker(kv_worker, receiver).await;
        });

        kv
    }
}

impl Kv {
    /// Insert (or update) a key.  Returns the timestamp that was written.
    ///
    /// The key is immediately broadcast to all gossip peers.
    pub async fn insert(&self, key: impl Into<String>) -> u64 {
        let key = key.into();
        let ts = now_millis();

        {
            let mut store = self.inner.store.lock().await;
            store.insert(key.clone(), ts);
        }

        // Best-effort broadcast, don't block the caller on gossip errors.
        self.send(Message::Insert {
            key: key.clone(),
            timestamp: ts,
        })
        .await;

        let _ = self.inner.updates.send(KvEvent {
            key,
            timestamp: ts,
            remote: false,
        });

        ts
    }

    /// All keys, sorted lexicographically.
    pub async fn query_all(&self) -> Vec<String> {
        self.inner.store.lock().await.keys().cloned().collect()
    }

    /// All keys that start with `prefix`, sorted lexicographically.
    pub async fn query_prefix(&self, prefix: &str) -> Vec<String> {
        self.inner
            .store
            .lock()
            .await
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Number of keys matching `prefix`.
    pub async fn count_prefix(&self, prefix: &str) -> usize {
        self.inner
            .store
            .lock()
            .await
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .count()
    }

    /// Check whether `key` exists.
    pub async fn contains(&self, key: &str) -> bool {
        self.inner.store.lock().await.contains_key(key)
    }

    /// Total number of keys.
    pub async fn len(&self) -> usize {
        self.inner.store.lock().await.len()
    }

    /// Check whether the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.inner.store.lock().await.is_empty()
    }

    /// Subscribe to live insert notifications (local + remote).
    pub fn subscribe(&self) -> broadcast::Receiver<KvEvent> {
        self.inner.updates.subscribe()
    }
}

impl Kv {
    /// Apply a remote key.  Returns `true` if the store was actually updated
    /// (i.e. the incoming timestamp was strictly newer).
    async fn apply(&self, key: &str, timestamp: u64) -> bool {
        let mut store = self.inner.store.lock().await;
        match store.get(key) {
            Some(&existing) if existing >= timestamp => false,
            _ => {
                store.insert(key.to_string(), timestamp);
                true
            }
        }
    }

    /// Snapshot the entire store for a `State` broadcast.
    async fn snapshot(&self) -> Vec<(String, u64)> {
        self.inner
            .store
            .lock()
            .await
            .iter()
            .map(|(k, &v)| (k.clone(), v))
            .collect()
    }

    /// Broadcast a full state dump.
    async fn broadcast_state(&self) {
        let entries = self.snapshot().await;
        if entries.is_empty() {
            return;
        }
        debug!("[Kv] Broadcasting state ({} keys)", entries.len());
        self.send(Message::State {
            entries,
            nonce: now_millis(),
        })
        .await;
    }

    /// Serialize + send via gossip.  Errors are logged, never propagated.
    async fn send(&self, msg: Message) {
        match postcard::to_allocvec(&msg) {
            Ok(bytes) => {
                if let Err(e) = self.inner.sender.broadcast(Bytes::from(bytes)).await {
                    warn!("[Kv] gossip broadcast failed: {}", e);
                }
            }
            Err(e) => warn!("[Kv] serialize failed: {}", e),
        }
    }
}

const JITTER_MIN_MS: u64 = 200;
const JITTER_MAX_MS: u64 = 2_000;
const PERIODIC_SYNC_SECS: u64 = 30;

async fn worker(kv: Kv, mut receiver: GossipReceiver) {
    use iroh_gossip::api::Event;

    let mut state_timer: Pin<Box<tokio::time::Sleep>> =
        Box::pin(tokio::time::sleep(Duration::from_secs(86400)));
    let mut state_pending = false;

    let mut periodic = tokio::time::interval(Duration::from_secs(PERIODIC_SYNC_SECS));
    periodic.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // On startup, schedule an initial state broadcast so any existing peers
    // learn about keys we may have inserted before the worker started.
    schedule_state(&mut state_timer, &mut state_pending);

    loop {
        tokio::select! {
            Some(event) = receiver.next() => {
                match event {
                    Ok(Event::Received(msg)) => {
                        handle_received(&kv, &msg.content, &mut state_pending).await;
                    }
                    Ok(Event::NeighborUp(peer)) => {
                        info!("[Kv] NeighborUp: {}, scheduling state broadcast", peer);
                        schedule_state(&mut state_timer, &mut state_pending);
                    }
                    Ok(Event::NeighborDown(peer)) => {
                        debug!("[Kv] NeighborDown: {}", peer);
                    }
                    Ok(Event::Lagged) => {
                        warn!("[Kv] Gossip lagged, scheduling state broadcast");
                        schedule_state(&mut state_timer, &mut state_pending);
                    }
                    Err(e) => warn!("[Kv] gossip stream error: {}", e),
                }
            }
            _ = &mut state_timer, if state_pending => {
                state_pending = false;
                kv.broadcast_state().await;
            }
            _ = periodic.tick() => {
                schedule_state(&mut state_timer, &mut state_pending);
            }
        }
    }
}

/// Reset the state-broadcast timer to fire after a random jitter.
///
/// If the timer was already pending it gets **replaced**, this combines
/// multiple rapid triggers (e.g. several `NeighborUp` events in a burst)
/// into one broadcast.
fn schedule_state(timer: &mut Pin<Box<tokio::time::Sleep>>, pending: &mut bool) {
    let jitter = rand::rng().random_range(JITTER_MIN_MS..=JITTER_MAX_MS);
    timer
        .as_mut()
        .reset(tokio::time::Instant::now() + Duration::from_millis(jitter));
    *pending = true;
    trace!("[Kv] State broadcast scheduled in {}ms", jitter);
}

/// Parse and apply one incoming gossip message.
///
/// When a `State` message is received from another peer, any pending local
/// state broadcast is cancelled, that peer already did the work.
async fn handle_received(kv: &Kv, raw: &[u8], state_pending: &mut bool) {
    let msg: Message = match postcard::from_bytes(raw) {
        Ok(m) => m,
        Err(e) => {
            trace!("[Kv] ignoring unparseable gossip message: {}", e);
            return;
        }
    };

    match msg {
        Message::Insert { key, timestamp } => {
            if kv.apply(&key, timestamp).await {
                debug!("[Kv] remote insert: {}", key);
                let _ = kv.inner.updates.send(KvEvent {
                    key,
                    timestamp,
                    remote: true,
                });
            }
        }
        Message::State { entries, .. } => {
            // Another peer already broadcast state, cancel our pending one.
            if *state_pending {
                debug!("[Kv] received remote state, cancelling pending local broadcast");
                *state_pending = false;
            }
            let mut applied = 0usize;
            for (key, timestamp) in entries {
                if kv.apply(&key, timestamp).await {
                    let _ = kv.inner.updates.send(KvEvent {
                        key,
                        timestamp,
                        remote: true,
                    });
                    applied += 1;
                }
            }
            if applied > 0 {
                debug!("[Kv] state merge: {} keys applied", applied);
            }
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
