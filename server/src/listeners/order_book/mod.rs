use crate::{
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::{
        Mutex,
        broadcast::Sender,
        mpsc::{UnboundedSender, unbounded_channel},
    },
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

// Circuit breaker configuration
const CIRCUIT_BREAKER_MAX_RESYNCS: usize = 5;
const CIRCUIT_BREAKER_WINDOW_SECS: u64 = 60;
const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 60; // 1 minute cooldown (reduced from 5 min since cache fix should make resyncs rare)

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    let ignore_spot = {
        let listener = listener.lock().await;
        listener.ignore_spot
    };

    // every so often, we fetch a new snapshot and the snapshot_fetch_task starts running.
    // Result is sent back along this channel (if error, we want to return to top level)
    let (snapshot_fetch_task_tx, mut snapshot_fetch_task_rx) = unbounded_channel::<Result<()>>();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshot_fetch_task_rx.recv() => {
                match snapshot_fetch_res {
                    None => {
                        error!("Snapshot fetch task sender dropped - this shouldn't happen");
                    }
                    Some(Err(err)) => {
                        warn!("Snapshot fetch failed: {err}. Will retry on next tick.");
                    }
                    Some(Ok(())) => {}
                }
            }
            _ = ticker.tick() => {
                let listener = listener.clone();
                let snapshot_fetch_task_tx = snapshot_fetch_task_tx.clone();
                fetch_snapshot(dir.clone(), listener, snapshot_fetch_task_tx, ignore_spot);
            }
            () = sleep(Duration::from_secs(60)) => {
                let listener = listener.lock().await;
                if listener.is_ready() {
                    warn!("No file system events received for 60 seconds. HL node may be stalled.");
                }
            }
        }
    }
}

fn fetch_snapshot(
    dir: PathBuf,
    listener: Arc<Mutex<OrderBookListener>>,
    tx: UnboundedSender<Result<()>>,
    ignore_spot: bool,
) {
    let tx = tx.clone();
    tokio::spawn(async move {
        // Fix #3: Start caching BEFORE the HTTP request
        // This ensures we capture updates that happen while the snapshot is being generated
        let state = {
            let mut listener = listener.lock().await;
            listener.begin_caching();
            listener.clone_state()
        };
        let state_height = state.as_ref().map(|s| s.height());
        info!("Started caching for snapshot validation (current state height: {:?})", state_height);

        let res = match process_rmp_file(&dir).await {
            Ok(output_fln) => {
                let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&output_fln).await;
                info!("Snapshot fetched");
                // sleep to let some updates build up after snapshot was taken
                sleep(Duration::from_secs(1)).await;
                let mut cache = {
                    let mut listener = listener.lock().await;
                    listener.take_cache()
                };
                info!("Cache has {} elements", cache.len());
                match snapshot {
                    Ok((height, expected_snapshot)) => {
                        if let Some(mut state) = state {
                            info!("Attempting to reach snapshot height {} from state height {}", height, state.height());
                            // Try to bring our state up to the snapshot height
                            while state.height() < height {
                                if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                                    let update_height = order_statuses.block_number();
                                    if let Err(e) = state.apply_updates(order_statuses, order_diffs) {
                                        // Failed to apply updates - resync from snapshot
                                        warn!("Failed to apply cached update at height {}: {}. Resyncing from snapshot...", update_height, e);
                                        listener.lock().await.resync_from_snapshot(expected_snapshot, height);
                                        return Ok::<(), Error>(());
                                    }
                                } else {
                                    // Not enough cached updates - resync from snapshot
                                    warn!(
                                        "Not enough cached updates to reach snapshot height {}. Current state: {}, cache exhausted. Resyncing from snapshot...",
                                        height, state.height()
                                    );
                                    listener.lock().await.resync_from_snapshot(expected_snapshot, height);
                                    return Ok::<(), Error>(());
                                }
                            }
                            if state.height() > height {
                                // Snapshot is lagging - this is unusual but not fatal
                                // Skip validation this round, next snapshot should be more recent
                                warn!("Fetched snapshot (height {}) lagging stored state (height {}). Skipping validation.", height, state.height());
                                return Ok::<(), Error>(());
                            }
                            let tracked = listener.lock().await.tracked_coin_set();
                            if tracked.is_empty() {
                                info!("No tracked coins, skipping validation at height {}", height);
                                return Ok::<(), Error>(());
                            }
                            let stored_snapshot = state.compute_snapshot().snapshot;
                            info!("Validating snapshot at height {}", height);

                            match validate_snapshot_consistency(&stored_snapshot, expected_snapshot.clone(), ignore_spot, &tracked) {
                                Ok(()) => {
                                    info!("Snapshot validation passed at height {}", height);
                                    Ok(())
                                }
                                Err(e) => {
                                    // Validation failed - resync from the expected snapshot
                                    warn!("Snapshot validation failed: {}. Resyncing from expected snapshot at height {}...", e, height);
                                    listener.lock().await.resync_from_snapshot(expected_snapshot, height);
                                    Ok(())
                                }
                            }
                        } else {
                            listener.lock().await.init_from_snapshot(expected_snapshot, height);
                            Ok(())
                        }
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                // HTTP request failed - stop caching
                let mut listener = listener.lock().await;
                listener.take_cache(); // Discard cache
                Err(err)
            }
        };
        let _unused = tx.send(res);
        Ok(())
    });
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
    resync_count: u64,
    last_resync_height: Option<u64>,
    fill_incomplete_line: String,
    order_status_incomplete_line: String,
    order_diff_incomplete_line: String,
    resync_timestamps: VecDeque<SystemTime>,
    degraded_mode_until: Option<SystemTime>,
    tracked_coins: HashMap<Coin, usize>,
    coin_universe: HashSet<String>,
}

impl OrderBookListener {
    pub(crate) fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
            resync_count: 0,
            last_resync_height: None,
            // Fix #1: Initialize empty incomplete line buffers
            fill_incomplete_line: String::new(),
            order_status_incomplete_line: String::new(),
            order_diff_incomplete_line: String::new(),
            // Fix #2: Initialize circuit breaker state
            resync_timestamps: VecDeque::new(),
            degraded_mode_until: None,
            tracked_coins: HashMap::new(),
            coin_universe: HashSet::new(),
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> &HashSet<String> {
        &self.coin_universe
    }

    pub(crate) fn track_coin(&mut self, coin: Coin) -> bool {
        let count = self.tracked_coins.entry(coin).or_insert(0);
        *count += 1;
        let is_new = *count == 1;
        if is_new {
            let tracked_set = self.tracked_coin_set();
            if let Some(state) = &mut self.order_book_state {
                state.set_tracked_coins(tracked_set);
            }
        }
        is_new
    }

    pub(crate) fn untrack_coin(&mut self, coin: &Coin) {
        if let Some(count) = self.tracked_coins.get_mut(coin) {
            *count -= 1;
            if *count == 0 {
                self.tracked_coins.remove(coin);
                let tracked_set = self.tracked_coin_set();
                if let Some(state) = &mut self.order_book_state {
                    state.remove_coin(coin);
                    state.set_tracked_coins(tracked_set);
                }
            }
        }
    }

    pub(crate) fn untrack_coins(&mut self, coins: &[Coin]) {
        for coin in coins {
            self.untrack_coin(coin);
        }
    }

    fn tracked_coin_set(&self) -> HashSet<Coin> {
        self.tracked_coins.keys().cloned().collect()
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        self.coin_universe = snapshot.as_ref().keys().map(|c| c.value()).collect();
        let tracked = self.tracked_coin_set();
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot, tracked);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    /// Check if circuit breaker is currently active (in degraded mode)
    fn is_circuit_breaker_active(&self) -> bool {
        if let Some(until) = self.degraded_mode_until {
            if SystemTime::now() < until {
                return true;
            }
        }
        false
    }

    /// Check if we should trip the circuit breaker based on recent resync frequency
    fn should_trip_circuit_breaker(&mut self) -> bool {
        let now = SystemTime::now();
        let window_start = now - Duration::from_secs(CIRCUIT_BREAKER_WINDOW_SECS);

        // Remove old timestamps outside the window
        while let Some(front) = self.resync_timestamps.front() {
            if *front < window_start {
                self.resync_timestamps.pop_front();
            } else {
                break;
            }
        }

        // Check if we've exceeded the threshold
        self.resync_timestamps.len() >= CIRCUIT_BREAKER_MAX_RESYNCS
    }

    /// Resync the orderbook state from a new snapshot after validation failure.
    /// This is Option C from the debugging guide - auto-resync on mismatch.
    ///
    /// Corner cases handled:
    /// 1. Circuit breaker prevents rapid resyncs (Fix #2)
    /// 2. Clears old state completely before reinitializing
    /// 3. Clears stale caches to prevent applying outdated updates
    /// 4. Tracks resync count for monitoring
    /// 5. Logs detailed information for debugging
    /// 6. If resync fails, state remains None and next snapshot will retry
    fn resync_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        let old_height = self.order_book_state.as_ref().map(|s| s.height());

        // Fix #2: Check circuit breaker - if active, skip resync
        if self.is_circuit_breaker_active() {
            warn!(
                "RESYNC BLOCKED: Circuit breaker active. Skipping resync at height {} (previous height: {:?}). \
                 Server is in degraded mode until {:?}. Serving potentially stale data.",
                height, old_height, self.degraded_mode_until
            );
            return;
        }

        // Record this resync attempt for circuit breaker
        self.resync_timestamps.push_back(SystemTime::now());
        self.resync_count += 1;

        // Fix #2: Check if we should trip the circuit breaker
        if self.should_trip_circuit_breaker() {
            let cooldown_until = SystemTime::now() + Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS);
            self.degraded_mode_until = Some(cooldown_until);
            error!(
                "CIRCUIT BREAKER TRIPPED: {} resyncs in {} seconds. \
                 Entering degraded mode for {} seconds. \
                 Server will serve potentially stale data and skip resyncs until {:?}. \
                 Total resyncs: {}",
                CIRCUIT_BREAKER_MAX_RESYNCS,
                CIRCUIT_BREAKER_WINDOW_SECS,
                CIRCUIT_BREAKER_COOLDOWN_SECS,
                cooldown_until,
                self.resync_count
            );
            // Don't perform this resync - we just tripped the breaker
            return;
        }

        warn!(
            "RESYNC #{}: Reinitializing orderbook from snapshot at height {} (previous height: {:?})",
            self.resync_count, height, old_height
        );

        // Check for rapid resync (potential infinite loop detection)
        if let Some(last_height) = self.last_resync_height {
            if height <= last_height + 10 {
                warn!(
                    "RESYNC WARNING: Rapid resync detected! Last resync at height {}, now at {}. \
                     This may indicate persistent state divergence issues. \
                     Recent resyncs in window: {}",
                    last_height, height, self.resync_timestamps.len()
                );
            }
        }
        self.last_resync_height = Some(height);

        // Clear the old state completely
        self.order_book_state = None;

        // Clear the update caches to avoid applying stale/conflicting updates
        // The caches may contain updates that conflict with the new snapshot
        self.order_diff_cache = BatchQueue::new();
        self.order_status_cache = BatchQueue::new();
        self.fetched_snapshot_cache = None;

        self.coin_universe = snapshot.as_ref().keys().map(|c| c.value()).collect();
        let tracked = self.tracked_coin_set();
        let new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot, tracked);

        // Note: Unlike init_from_snapshot, we don't apply cached updates here
        // because we just cleared them. The next round of file updates will
        // naturally populate the caches and be applied via receive_batch.

        self.order_book_state = Some(new_order_book);

        // Broadcast L4 snapshot to all connected clients so they can resync their state
        // Compute snapshot first to avoid borrow conflict with internal_message_tx
        let snapshot_to_send = self.compute_snapshot();
        if let (Some(internal_message_tx), Some(snapshot)) = (&self.internal_message_tx, snapshot_to_send) {
            let msg = Arc::new(InternalMessage::L4Snapshot { snapshot });
            let _unused = internal_message_tx.send(msg);
        }

        info!(
            "RESYNC #{}: Orderbook resynced successfully at height {}. Total resyncs: {}",
            self.resync_count, height, self.resync_count
        );
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                new_file.seek(SeekFrom::End(0))?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        // Fix #1: Process remaining data from old file with line-buffered reading
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                // Use line-buffered processing - any incomplete line is saved
                self.process_data_line_buffered(buf, event_source)?;
            }
        }
        // Clear incomplete line buffer when switching to new file
        // (incomplete data from old file is lost, but that's expected at file boundaries)
        self.incomplete_line_mut(event_source).clear();
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        // Fix #1: Delegate to line-buffered implementation
        self.process_data_line_buffered(data, event_source)
    }
}

// Fix #1: Line-buffered reading implementation
impl OrderBookListener {
    /// Get mutable reference to the incomplete line buffer for an event source
    fn incomplete_line_mut(&mut self, event_source: EventSource) -> &mut String {
        match event_source {
            EventSource::Fills => &mut self.fill_incomplete_line,
            EventSource::OrderStatuses => &mut self.order_status_incomplete_line,
            EventSource::OrderDiffs => &mut self.order_diff_incomplete_line,
        }
    }

    /// Process data with line-buffered reading.
    /// Only processes complete lines (ending with \n).
    /// Incomplete trailing data is saved for the next read.
    fn process_data_line_buffered(&mut self, data: String, event_source: EventSource) -> Result<()> {
        // Prepend any incomplete line from previous read
        let incomplete = std::mem::take(self.incomplete_line_mut(event_source));
        let full_data = if incomplete.is_empty() {
            data
        } else {
            incomplete + &data
        };

        // Check if data ends with newline (complete) or not (incomplete trailing line)
        let ends_with_newline = full_data.ends_with('\n');

        let mut lines: Vec<&str> = full_data.lines().collect();

        // If data doesn't end with newline, the last "line" is incomplete
        // Save it for next read and don't process it
        if !ends_with_newline && !lines.is_empty() {
            let incomplete_line = lines.pop().unwrap_or("");
            *self.incomplete_line_mut(event_source) = incomplete_line.to_string();
        }

        // Process only complete lines
        for line in lines {
            if line.is_empty() {
                continue;
            }

            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };

            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    // This should be rare now - only happens if the node writes malformed JSON
                    // (not due to partial reads, which are now handled by line buffering)
                    let line_preview = if line.len() > 100 { &line[..100] } else { line };
                    error!(
                        "{event_source} JSON parse error on COMPLETE line: {err}, height: {:?}, line preview: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        line_preview,
                    );
                    // Skip this malformed line and continue with next
                    // (Don't seek back - the line was complete but malformed)
                    continue;
                }
            };

            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }

            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }

        // Send L2 snapshot if ready
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                let tx = tx.clone();
                tokio::spawn(async move {
                    let snapshot = Arc::new(InternalMessage::Snapshot { l2_snapshots: snapshot.1, time: snapshot.0 });
                    let _unused = tx.send(snapshot);
                });
            }
        }
        Ok(())
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);

impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
    L4Snapshot { snapshot: TimedSnapshots },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}
