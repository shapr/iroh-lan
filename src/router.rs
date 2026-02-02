use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::Ipv4Addr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use ed25519_dalek::SigningKey;
use futures::StreamExt;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::{AuthorId, Entry, NamespaceSecret, api::Doc, protocol::Docs, store::Query};
use tokio::sync::watch;

use anyhow::{Context, Result, bail};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::{
    api::{GossipReceiver, GossipSender},
    net::Gossip,
};
use iroh_topic_tracker::{
    TopicDiscoveryConfig, TopicDiscoveryExt, TopicDiscoveryHandle, TopicDiscoveryHook,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tracing::{debug, info, trace, warn};

use actor_helper::{Action, Actor, Handle, act, act_ok};

#[derive(Debug, Clone)]
pub struct Builder {
    topic_discovery_hook: TopicDiscoveryHook,
    entry_name: String,
    secret_key: SecretKey,
    password: String,
    endpoint: Option<Endpoint>,
    gossip: Option<Gossip>,
    docs: Option<Docs>,
    blobs: MemStore,
}

impl Builder {
    pub fn new(topic_discovery_hook: TopicDiscoveryHook) -> Builder {
        Builder {
            topic_discovery_hook,
            entry_name: String::default(),
            secret_key: SecretKey::generate(&mut rand::rng()),
            password: String::default(),
            endpoint: None,
            gossip: None,
            docs: None,
            blobs: MemStore::new(),
        }
    }

    pub fn entry_name(mut self, entry_name: &str) -> Self {
        self.entry_name = entry_name.to_string();
        self
    }

    pub fn secret_key(mut self, secret_key: SecretKey) -> Self {
        self.secret_key = secret_key;
        self
    }

    pub fn password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self
    }

    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn gossip(mut self, gossip: Gossip) -> Self {
        self.gossip = Some(gossip);
        self
    }

    pub fn docs(mut self, docs: Docs) -> Self {
        self.docs = Some(docs);
        self
    }

    pub fn blobs(mut self, blobs: MemStore) -> Self {
        self.blobs = blobs;
        self
    }

    pub async fn build(&self) -> Result<Router> {
        let endpoint = if let Some(ep) = &self.endpoint {
            ep.clone()
        } else {
            bail!("endpoint must be set");
        };
        let gossip = if let Some(g) = &self.gossip {
            g.clone()
        } else {
            bail!("gossip must be set");
        };
        let docs = if let Some(d) = &self.docs {
            d.clone()
        } else {
            bail!("docs must be set");
        };
        let blobs = self.blobs.clone();

        let topic_initials = format!("lanparty-{}", self.entry_name);
        let secret_initials = format!("{topic_initials}-{}-secret", self.password)
            .as_bytes()
            .to_vec();

        let mut topic_hasher = sha2::Sha512::new();
        topic_hasher.update("iroh-lan-topic");
        topic_hasher.update(&secret_initials);
        let topic_hash: [u8; 32] = topic_hasher.finalize()[..32].try_into()?;

        let signing_key = SigningKey::from_bytes(&self.secret_key.to_bytes());
        let topic_discovery_config =
            TopicDiscoveryConfig::new(signing_key, self.topic_discovery_hook.clone());
        let (gossip_sender, gossip_receiver, topic_handle) = loop {
            if let Ok((gossip_sender, gossip_receiver, topic_handle)) = gossip
                .subscribe_with_discovery_joined(
                    topic_hash.to_vec(),
                    vec![],
                    topic_discovery_config.clone(),
                )
                .await
            {
                break (gossip_sender, gossip_receiver, Some(topic_handle));
            } else {
                warn!("Failed to join topic; retrying in 2 second");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        };

        let doc_peers = gossip_receiver
            .neighbors()
            .map(EndpointAddr::new)
            .collect::<Vec<_>>();
        let (doc_peers_tx, _doc_peers_rx) = watch::channel(doc_peers.clone());

        debug!("[Doc peers]: {:?}", doc_peers);

        let author_id = docs.author_create().await?;
        let doc = docs
            .import(iroh_docs::DocTicket {
                capability: iroh_docs::Capability::Write(NamespaceSecret::from_bytes(&topic_hash)),
                nodes: doc_peers.clone(),
            })
            .await?;

        let wait_start = tokio::time::Instant::now();
        while match doc.get_sync_peers().await {
            Ok(Some(peers)) => peers.is_empty(),
            Ok(None) => true,
            Err(_) => true,
        } {
            if wait_start.elapsed() > Duration::from_secs(30) {
                warn!("Doc sync peers not available yet; continuing without peers");
                break;
            }
            debug!("Waiting for doc to be ready...");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let (api, rx) = Handle::channel();
        tokio::spawn(async move {
            let mut router_actor = RouterActor {
                _gossip_sender: gossip_sender,
                gossip_monitor: gossip_receiver,
                author_id,
                _docs: docs,
                doc,
                endpoint_id: endpoint.id(),
                _topic: topic_handle,
                rx,
                blobs,
                my_ip: RouterIp::NoIp,
                assignments: BTreeMap::new(),
                candidates: BTreeMap::new(),
                doc_peers_tx,
            };
            router_actor.run().await
        });

        Ok(Router { api })
    }
}

#[derive(Debug, Clone)]
pub struct Router {
    api: Handle<RouterActor, anyhow::Error>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterIp {
    NoIp,
    AquiringIp(IpCandidate, tokio::time::Instant),
    VerifyingIp(Ipv4Addr, tokio::time::Instant),
    AssignedIp(Ipv4Addr),
}

#[derive(Debug)]
struct RouterActor {
    pub(crate) rx: actor_helper::Receiver<Action<RouterActor>>,

    pub _gossip_sender: GossipSender,
    pub gossip_monitor: GossipReceiver,

    pub(crate) blobs: MemStore,
    pub(crate) _docs: Docs,
    pub(crate) doc: Doc,
    pub(crate) author_id: AuthorId,

    pub endpoint_id: EndpointId,
    pub(crate) _topic: Option<TopicDiscoveryHandle>,

    pub my_ip: RouterIp,

    assignments: BTreeMap<Ipv4Addr, IpAssignment>,
    candidates: BTreeMap<(Ipv4Addr, EndpointId), IpCandidate>,
    doc_peers_tx: watch::Sender<Vec<EndpointAddr>>,
}

#[derive(Debug)]
enum DocCacheUpdate {
    UpsertAssignment(IpAssignment),
    RemoveAssignment(Ipv4Addr),
    UpsertCandidate(IpCandidate),
    RemoveCandidate(Ipv4Addr, EndpointId),
}

async fn handle_doc_entry(
    entry: Entry,
    blobs: &MemStore,
    updates: &tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>,
    pending: &mut HashMap<iroh_blobs::Hash, Vec<Entry>>,
    download_expected: bool,
) -> bool {
    let key = match std::str::from_utf8(entry.key()) {
        Ok(k) => k,
        Err(e) => {
            warn!("Invalid entry key utf8: {}", e);
            return false;
        }
    };

    let assigned_prefix = key_ip_assigned_prefix();
    let candidate_prefix = key_ip_candidate_prefix();

    if entry.content_len() == 0 {
        if key.starts_with(&assigned_prefix) {
            if let Some(ip_str) = key.strip_prefix(&assigned_prefix)
                && let Ok(ip) = ip_str.parse::<Ipv4Addr>()
            {
                updates.send(DocCacheUpdate::RemoveAssignment(ip)).ok();
            }
        } else if key.starts_with(&candidate_prefix) {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 5
                && let (Ok(ip), Ok(eid)) =
                    (parts[3].parse::<Ipv4Addr>(), parts[4].parse::<EndpointId>())
            {
                updates.send(DocCacheUpdate::RemoveCandidate(ip, eid)).ok();
            }
        }
        return false;
    }

    let val_bytes = match blobs.get_bytes(entry.content_hash()).await {
        Ok(b) => {
            debug!("Blob found for entry key {}", entry.content_hash());
            b
        }
        Err(e) => {
            if download_expected {
                warn!(
                    "Blob not found for entry key {} ({}), deferring processing",
                    entry.content_hash(),
                    e
                );
            }
            let hash = entry.content_hash();
            pending.entry(hash).or_default().push(entry);
            let err_str = e.to_string();
            debug!("Blob get_bytes failed for {}: {}", hash, err_str);
            return true;
        }
    };

    // only tag after get_bytes succeeds
    blobs.tags().create(entry.content_hash()).await.ok();

    if key.starts_with(&assigned_prefix) {
        match postcard::from_bytes::<IpAssignment>(&val_bytes) {
            Ok(val) => {
                updates.send(DocCacheUpdate::UpsertAssignment(val)).ok();
            }
            Err(e) => warn!("Failed to deserialize assignment: {}", e),
        }
    } else if key.starts_with(&candidate_prefix) {
        match postcard::from_bytes::<IpCandidate>(&val_bytes) {
            Ok(val) => {
                updates.send(DocCacheUpdate::UpsertCandidate(val)).ok();
            }
            Err(e) => warn!("Failed to deserialize candidate: {}", e),
        }
    }
    false
}

fn schedule_resync_doc(
    doc: Doc,
    peers_rx: watch::Receiver<Vec<EndpointAddr>>,
    resync_in_flight: Arc<AtomicBool>,
) {
    if resync_in_flight
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    tokio::spawn(async move {
        let peers = peers_rx.borrow().clone();
        if !peers.is_empty() {
            let res = tokio::time::timeout(Duration::from_secs(5), doc.start_sync(peers)).await;
            if let Err(e) = res {
                warn!("Doc resync timed out: {}", e);
            } else if let Ok(Err(e)) = res {
                warn!("Failed to resync doc for pending blobs: {}", e);
            }
        }
        resync_in_flight.store(false, Ordering::SeqCst);
    });
}

async fn run_doc_worker(
    doc: Doc,
    blobs: MemStore,
    doc_peers_rx: watch::Receiver<Vec<EndpointAddr>>,
    updates: tokio::sync::mpsc::UnboundedSender<DocCacheUpdate>,
) {
    let mut pending_blobs: HashMap<iroh_blobs::Hash, Vec<Entry>> = HashMap::new();
    let mut retry_tick = tokio::time::interval(Duration::from_secs(5));
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let resync_in_flight = Arc::new(AtomicBool::new(false));

    match doc.get_many(Query::all()).await {
        Ok(stream) => {
            let entries = stream.collect::<Vec<_>>().await;
            for entry in entries.into_iter().flatten() {
                let needs_resync =
                    handle_doc_entry(entry, &blobs, &updates, &mut pending_blobs, true).await;
                if needs_resync {
                    schedule_resync_doc(
                        doc.clone(),
                        doc_peers_rx.clone(),
                        resync_in_flight.clone(),
                    );
                }
            }
        }
        Err(e) => warn!("Failed to populate cache: {}", e),
    }

    let mut docs_sub = match doc.subscribe().await {
        Ok(sub) => sub,
        Err(e) => {
            warn!("Docs subscription failed: {}", e);
            return;
        }
    };

    loop {
        tokio::select! {
            Some(event) = docs_sub.next() => {
                match event {
                    Ok(e) => match e {
                        iroh_docs::engine::LiveEvent::InsertLocal { entry }
                        | iroh_docs::engine::LiveEvent::InsertRemote { entry, .. } => {
                            let needs_resync =
                                handle_doc_entry(entry, &blobs, &updates, &mut pending_blobs, false).await;
                            if needs_resync {
                                schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                            }
                        }
                        iroh_docs::engine::LiveEvent::ContentReady { hash } => {
                            if let Some(entries) = pending_blobs.remove(&hash) {
                                for entry in entries {
                                    let needs_resync = handle_doc_entry(
                                        entry,
                                        &blobs,
                                        &updates,
                                        &mut pending_blobs,
                                        true,
                                    )
                                    .await;
                                    if needs_resync {
                                        schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                                    }
                                }
                            }
                        }
                        iroh_docs::engine::LiveEvent::PendingContentReady => {
                            if !pending_blobs.is_empty() {
                                schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());

                                let pending = std::mem::take(&mut pending_blobs);
                                for (_hash, entries) in pending {
                                    for entry in entries {
                                        let needs_resync = handle_doc_entry(
                                            entry,
                                            &blobs,
                                            &updates,
                                            &mut pending_blobs,
                                            true,
                                        )
                                        .await;
                                        if needs_resync {
                                            schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(e) => warn!("Docs subscription stream error: {:?}", e),
                }
            }
            _ = retry_tick.tick() => {
                if !pending_blobs.is_empty() {
                    debug!("Retry tick: {} pending blobs", pending_blobs.len());
                    schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                    let pending = std::mem::take(&mut pending_blobs);
                    for (_hash, entries) in pending {
                        for entry in entries {
                            let needs_resync = handle_doc_entry(
                                entry,
                                &blobs,
                                &updates,
                                &mut pending_blobs,
                                true,
                            )
                            .await;
                            if needs_resync {
                                schedule_resync_doc(doc.clone(), doc_peers_rx.clone(), resync_in_flight.clone());
                            }
                        }
                    }
                }
            }
            else => break,
        }
    }
}

impl Router {
    pub fn builder(topic_discovery_hook: TopicDiscoveryHook) -> Builder {
        Builder::new(topic_discovery_hook)
    }

    pub async fn get_ip_state(&self) -> Result<RouterIp> {
        self.api
            .call(act_ok!(actor => async move {
                    actor.my_ip.clone()
            }))
            .await
    }

    pub async fn get_node_id(&self) -> Result<EndpointId> {
        self.api
            .call(act_ok!(actor => async move {
                    actor.endpoint_id
            }))
            .await
    }

    pub async fn get_ip_from_endpoint_id(&self, endpoint_id: EndpointId) -> Result<Ipv4Addr> {
        self.api
            .call(act!(actor => actor.get_ip_from_endpoint_id(endpoint_id)))
            .await
    }

    pub async fn get_endpoint_id_from_ip(&self, ip: Ipv4Addr) -> Result<EndpointId> {
        self.api
            .call(act!(actor => actor.get_endpoint_id_from_ip(ip)))
            .await
    }

    pub async fn get_peers(&self) -> Result<Vec<(EndpointId, Option<Ipv4Addr>)>> {
        self.api
            .call(act!(actor => async move {
                let mut map: BTreeMap<EndpointId, Option<Ipv4Addr>> = BTreeMap::new();

                let assignments = actor.read_all_ip_assignments();
                for a in assignments {
                    map.insert(a.endpoint_id, Some(a.ip));
                }

                let cands = actor.read_all_ip_candidates();
                for c in cands {
                    map.entry(c.endpoint_id).or_insert(None);
                }

                map.remove(&actor.endpoint_id);

                Ok(map.into_iter().collect())
            }))
            .await
    }

    pub async fn close(&self) -> Result<()> {
        self.api.call(act!(actor => actor.doc.close())).await
    }
}

impl Actor<anyhow::Error> for RouterActor {
    async fn run(&mut self) -> Result<()> {
        let mut ip_tick = tokio::time::interval(Duration::from_millis(500));
        ip_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let cleanup_interval_secs = 5 + rand::rng().random_range(0..10);
        let mut cleanup_tick = tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
        cleanup_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        debug!("RouterActor started. EndpointId: {}", self.endpoint_id);

        let mut last_ip_state = self.my_ip.clone();

        let (doc_update_tx, mut doc_update_rx) = tokio::sync::mpsc::unbounded_channel();
        let doc = self.doc.clone();
        let blobs = self.blobs.clone();
        let doc_peers_rx = self.doc_peers_tx.subscribe();
        tokio::spawn(async move {
            run_doc_worker(doc, blobs, doc_peers_rx, doc_update_tx).await;
        });
        loop {
            tokio::select! {
                Ok(action) = self.rx.recv_async() => {
                    trace!("Action received");
                    action(self).await;
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Received Ctrl-C, stopping RouterActor");
                    break
                }

                Some(event) = self.gossip_monitor.next() => {
                    match event {
                        Ok(e) => {
                            if rand::rng().random_bool(0.1) {
                                warn!("Gossip monitor: Unhandled event (potential backpressure source): {:?}", e);
                            }
                        }
                        Err(e) => warn!("Gossip monitor stream error: {:?}", e),
                    }
                    let peers = self
                        .gossip_monitor
                        .neighbors()
                        .map(EndpointAddr::new)
                        .collect::<Vec<_>>();
                    let _ = self.doc_peers_tx.send(peers);
                }

                // advance ip state machine
                _ = ip_tick.tick() => {
                    trace!("IP tick");
                    match self.advance_ip_state().await {
                        Ok(_) => if self.my_ip != last_ip_state {
                            info!("Router IP state changed: {:?} -> {:?}", last_ip_state, self.my_ip);
                            last_ip_state = self.my_ip.clone();
                        },
                        Err(e) => {
                            warn!("Error advancing IP state: {}", e);
                        }
                    }
                }

                _ = cleanup_tick.tick() => {
                    trace!("Cleanup tick");
                    if let Err(e) = self.perform_cleanup().await {
                        warn!("Error performing cleanup: {}", e);
                    }
                }

                Some(update) = doc_update_rx.recv() => {
                    match update {
                        DocCacheUpdate::UpsertAssignment(val) => {
                            self.assignments.insert(val.ip, val);
                        }
                        DocCacheUpdate::RemoveAssignment(ip) => {
                            self.assignments.remove(&ip);
                        }
                        DocCacheUpdate::UpsertCandidate(val) => {
                            self.candidates.insert((val.ip, val.endpoint_id), val);
                        }
                        DocCacheUpdate::RemoveCandidate(ip, endpoint_id) => {
                            self.candidates.remove(&(ip, endpoint_id));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn current_time() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpAssignment {
    pub ip: Ipv4Addr,
    pub endpoint_id: EndpointId,
    pub last_updated: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpCandidate {
    pub ip: Ipv4Addr,
    pub endpoint_id: EndpointId,
    pub last_updated: u64,
}

fn key_ip_assigned(ip: Ipv4Addr) -> String {
    format!("/assigned/ip/{ip}")
}

fn key_ip_assigned_prefix() -> String {
    "/assigned/ip/".to_string()
}

fn key_ip_candidate(ip: Ipv4Addr, endpoint_id: EndpointId) -> String {
    format!("/candidates/ip/{ip}/{endpoint_id}")
}

fn key_ip_candidate_prefix() -> String {
    "/candidates/ip/".to_string()
}

impl RouterActor {
    const ASSIGNMENT_STALE_SECS: u64 = 300;
    const CANDIDATE_STALE_SECS: u64 = 60;

    fn read_all_ip_assignments(&self) -> Vec<IpAssignment> {
        self.assignments.values().cloned().collect()
    }

    fn read_all_ip_candidates(&self) -> Vec<IpCandidate> {
        self.candidates.values().cloned().collect()
    }

    async fn clear_ip_candidate(&mut self, ip: Ipv4Addr, endpoint_id: EndpointId) -> Result<()> {
        debug!("Clearing IP candidate {} for {}", ip, endpoint_id);
        self.doc
            .del(self.author_id, key_ip_candidate(ip, endpoint_id))
            .await?;
        Ok(())
    }

    // Assigned IPs
    fn read_ip_assignment(&self, ip: Ipv4Addr) -> Result<IpAssignment> {
        self.assignments
            .get(&ip)
            .cloned()
            .context("no assignment found")
    }

    // Candidate IPs
    fn read_ip_candidates(&self, ip: Ipv4Addr) -> Result<Vec<IpCandidate>> {
        let now = current_time();
        let filtered = self
            .candidates
            .values()
            .filter(|c| c.ip == ip)
            .filter(|candidate| {
                now.saturating_sub(candidate.last_updated) <= Self::CANDIDATE_STALE_SECS
            })
            .cloned()
            .collect::<Vec<_>>();
        debug!("candidates for {ip}: {}", filtered.len());
        Ok(filtered)
    }

    // write ip assigned
    async fn write_ip_assignment(&mut self, ip: Ipv4Addr, endpoint_id: EndpointId) -> Result<()> {
        info!("Attempting to assign IP {} to {}", ip, endpoint_id);

        let existing_assignment = self.read_ip_assignment(ip).ok();

        if let Some(existing) = &existing_assignment {
            if existing.endpoint_id != endpoint_id {
                anyhow::bail!("ip already assigned to another node");
            }
        } else {
            // New assignment. check for multiple candidates
            let candidates = self.read_ip_candidates(ip)?;
            if candidates.is_empty() {
                bail!("no candidates for this ip");
            }

            if candidates
                .iter()
                .any(|c| c.endpoint_id != self.endpoint_id && self.endpoint_id < c.endpoint_id)
            {
                self.clear_ip_candidate(ip, endpoint_id).await?;
                bail!("another candidate with higher endpoint_id exists");
            }
        }

        // We are the chosen one!
        debug!("Winning candidate for IP {}. Writing assignment.", ip);
        let data = postcard::to_stdvec(&IpAssignment {
            ip,
            endpoint_id,
            last_updated: current_time(),
        })?;
        self.doc
            .set_bytes(self.author_id, key_ip_assigned(ip), data)
            .await?;

        self.clear_ip_candidate(ip, endpoint_id).await.ok();
        Ok(())
    }

    // write ip candidate
    async fn write_ip_candidate(
        &mut self,
        ip: Ipv4Addr,
        endpoint_id: EndpointId,
    ) -> Result<IpCandidate> {
        debug!("Writing IP candidate {} for {}", ip, endpoint_id);
        // already assigned? don't write
        if self.read_ip_assignment(ip).is_ok() {
            anyhow::bail!("ip already assigned");
        }

        // read existing candidates; Ok(vec![]) when none exist
        let candidates = self.read_ip_candidates(ip).unwrap_or_default();

        // if someone else is already a candidate, treat as contested and skip writing
        if candidates.iter().any(|c| c.endpoint_id != endpoint_id) {
            warn!("Contested IP candidate {}. Another candidate exists.", ip);
            anyhow::bail!("ip candidate already exists");
        }

        // idempotent for our own node_id
        let candidate = IpCandidate {
            ip,
            endpoint_id,
            last_updated: current_time(),
        };
        let data = postcard::to_stdvec(&candidate)?;
        self.doc
            .set_bytes(self.author_id, key_ip_candidate(ip, endpoint_id), data)
            .await?;
        Ok(candidate)
    }

    async fn get_endpoint_id_from_ip(&mut self, ip: Ipv4Addr) -> Result<EndpointId> {
        self.read_ip_assignment(ip).map(|a| a.endpoint_id)
    }

    async fn get_ip_from_endpoint_id(&mut self, endpoint_id: EndpointId) -> Result<Ipv4Addr> {
        self.read_all_ip_assignments()
            .iter()
            .find(|assignment| assignment.endpoint_id == endpoint_id)
            .map(|a| a.ip)
            .ok_or_else(|| anyhow::anyhow!("no ip found for endpoint_id"))
    }
}

impl RouterActor {
    async fn advance_ip_state(&mut self) -> Result<bool> {
        trace!("Advancing IP state. Current: {:?}", self.my_ip);
        match self.my_ip.clone() {
            RouterIp::NoIp => {
                let next_ip = self.get_next_ip().await?;
                info!("Trying to acquire IP candidate: {}", next_ip);

                self.my_ip = RouterIp::AquiringIp(
                    self.write_ip_candidate(next_ip, self.endpoint_id).await?,
                    tokio::time::Instant::now(),
                );
                Ok(false)
            }
            RouterIp::AquiringIp(ip_candidate, start_time) => {
                let elapsed = start_time.elapsed();

                if let Ok(candidates) = self.read_ip_candidates(ip_candidate.ip)
                    && candidates.iter().any(|c| {
                        c.endpoint_id != self.endpoint_id && self.endpoint_id < c.endpoint_id
                    })
                {
                    warn!(
                        "Conflict detected for {:?} during acquisition wait. Aborting.",
                        ip_candidate.ip
                    );
                    self.clear_ip_candidate(ip_candidate.ip, ip_candidate.endpoint_id)
                        .await?;
                    self.my_ip = RouterIp::NoIp;
                    return Ok(false);
                }

                // Wait at least 5 seconds
                if elapsed > Duration::from_secs(5) {
                    // Jitter: 20% chance to proceed per tick (approx 500ms)
                    if rand::rng().random_bool(0.2) {
                        info!(
                            "Attempting to finalize IP assignment for {}",
                            ip_candidate.ip
                        );
                        if self
                            .write_ip_assignment(ip_candidate.ip, ip_candidate.endpoint_id)
                            .await
                            .is_ok()
                        {
                            info!(
                                "Written assignment for IP: {}. Verifying...",
                                ip_candidate.ip
                            );
                            self.my_ip =
                                RouterIp::VerifyingIp(ip_candidate.ip, tokio::time::Instant::now());
                        } else {
                            warn!(
                                "Failed to write assignment for {}. Restarting negotiation.",
                                ip_candidate.ip
                            );
                            self.clear_ip_candidate(ip_candidate.ip, ip_candidate.endpoint_id)
                                .await
                                .ok();
                            self.my_ip = RouterIp::NoIp;
                        }
                    }
                }
                Ok(false)
            }
            RouterIp::VerifyingIp(ip, start_time) => {
                // Wait 2 seconds to ensure propagation
                if start_time.elapsed() > Duration::from_secs(2) {
                    trace!("Verifying IP assignment logic for {}", ip);
                    let assignment = self.read_ip_assignment(ip);
                    match assignment {
                        Ok(a) if a.endpoint_id == self.endpoint_id => {
                            info!("Verified ownership of IP: {}", ip);
                            self.my_ip = RouterIp::AssignedIp(ip);
                        }
                        _ => {
                            warn!(
                                "Verification failed for IP: {}. Lost to another node or ghost.",
                                ip
                            );
                            self.my_ip = RouterIp::NoIp;
                        }
                    }
                }
                Ok(false)
            }
            RouterIp::AssignedIp(my_ip) => {
                match self.read_ip_assignment(my_ip) {
                    Ok(ip_assignment) => {
                        if ip_assignment.endpoint_id != self.endpoint_id {
                            self.my_ip = RouterIp::NoIp;
                            warn!(
                                "Lost IP assignment for {}. Expected owner: {}, Found: {}. Restarting negotiation.",
                                my_ip, self.endpoint_id, ip_assignment.endpoint_id
                            );
                            Ok(false)
                        } else {
                            // Refresh if needed (every 30s)
                            if current_time() - ip_assignment.last_updated > 30 {
                                info!("Refreshing IP assignment for {}", my_ip);
                                if let Err(e) =
                                    self.write_ip_assignment(my_ip, self.endpoint_id).await
                                {
                                    warn!("Failed to refresh IP assignment: {}", e);
                                    // Don't lose IP immediately, retry next tick
                                }
                            }
                            Ok(true)
                        }
                    }
                    Err(_) => {
                        self.my_ip = RouterIp::NoIp;
                        warn!("Assignment lost/deleted for {}. Restarting.", my_ip);
                        Ok(false)
                    }
                }
            }
        }
    }

    async fn get_next_ip(&mut self) -> Result<Ipv4Addr> {
        let all_assigned = self.read_all_ip_assignments();
        let all_candidates = self.read_all_ip_candidates();

        let mut used_ips = all_assigned
            .iter()
            .map(|ip_assignment| ip_assignment.ip)
            .collect::<HashSet<_>>();
        used_ips.extend(
            all_candidates
                .iter()
                .map(|ip_candidate| ip_candidate.ip)
                .collect::<HashSet<_>>(),
        );

        // Scan for the first available IP starting from 172.30.0.2
        for i in 2u16..65534 {
            let octet3 = (i >> 8) as u8;
            let octet4 = (i & 0xFF) as u8;

            // Skip network/broadcast addresses if needed, though 172.30.0.0/16 is a large subnet.
            // Keeping it simple: avoid .0 and .255 in the last octet to be safe for standard /24 clients
            if octet4 == 0 || octet4 == 255 {
                continue;
            }

            let ip = Ipv4Addr::new(172, 30, octet3, octet4);
            if !used_ips.contains(&ip) {
                return Ok(ip);
            }
        }

        bail!("no more ips available");
    }

    async fn perform_cleanup(&mut self) -> Result<()> {
        let now = current_time();

        // Cleanup assignments
        for a in self.read_all_ip_assignments() {
            if now.saturating_sub(a.last_updated) > Self::ASSIGNMENT_STALE_SECS {
                /*if a.endpoint_id == self.endpoint_id {
                    error!(
                        "Self-eviction: Expiring own IP assignment for {} (Age: {}s)",
                        a.ip,
                        now - a.last_updated
                    );
                }*/
                info!(
                    "Removing stale IP assignment for {} (Endpoint: {}, Age: {}s)",
                    a.ip,
                    a.endpoint_id,
                    now - a.last_updated
                );
                if let Err(e) = self.doc.del(self.author_id, key_ip_assigned(a.ip)).await {
                    warn!("Failed to delete stale IP {}: {}", a.ip, e);
                } else {
                    info!("Cleaned up assignment: {:?}", a);
                }
            }
        }

        // Cleanup candidates
        for c in self.read_all_ip_candidates() {
            if now.saturating_sub(c.last_updated) > Self::CANDIDATE_STALE_SECS {
                info!(
                    "Removing stale IP candidate for {} (last updated {}s ago)",
                    c.ip,
                    now - c.last_updated
                );
                self.clear_ip_candidate(c.ip, c.endpoint_id).await.ok();
            }
        }
        Ok(())
    }
}
