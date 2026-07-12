// SPDX-License-Identifier: Apache-2.0

use std::alloc::{alloc, alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::collections::VecDeque;
use std::future::Future;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use dust_dds::infrastructure::error::DdsError;
use dust_dds::infrastructure::qos::{DataReaderQos, DataWriterQos};
use dust_dds::infrastructure::qos_policy::{HistoryQosPolicyKind, ReliabilityQosPolicyKind};
use up_rust::{UCode, UStatus, UUri};

static NEXT_ORIGIN: AtomicU64 = AtomicU64::new(1);
const MAX_HEALTH_EVENTS: usize = 64;

/// DDS endpoint reliability policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reliability {
    /// Reliable DDS delivery.
    Reliable,
    /// Best-effort DDS delivery.
    BestEffort,
}

/// Configurable `QoS` used by all three carriage families.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdsQos {
    /// Reader and writer reliability.
    pub reliability: Reliability,
    /// `KEEP_LAST` history depth.
    pub history_depth: u32,
}

impl Default for DdsQos {
    fn default() -> Self {
        Self {
            reliability: Reliability::Reliable,
            history_depth: 32,
        }
    }
}

/// Runtime configuration shared by classic, owned, and zero-copy families.
#[derive(Clone, Debug)]
pub struct DdsConfig {
    /// DDS domain ID.
    pub domain_id: i32,
    /// Stable identity of this transport instance for exact self suppression.
    pub origin_id: String,
    /// DDS endpoint `QoS`.
    pub qos: DdsQos,
    /// Maximum queued callback jobs.
    pub dispatch_capacity: usize,
    /// Maximum samples taken in one poll.
    pub max_samples_per_poll: i32,
    /// Maximum idle wait between nonblocking reader polls.
    pub poll_interval: Duration,
}

impl DdsConfig {
    /// Constructs a configuration with a process-unique origin.
    #[must_use]
    pub fn new(domain_id: i32) -> Self {
        let sequence = NEXT_ORIGIN.fetch_add(1, Ordering::Relaxed);
        Self {
            domain_id,
            origin_id: format!("{}-{sequence}", std::process::id()),
            qos: DdsQos::default(),
            dispatch_capacity: 256,
            max_samples_per_poll: 64,
            poll_interval: Duration::from_millis(2),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), UStatus> {
        if self.origin_id.is_empty() || self.origin_id.len() > 255 {
            return Err(invalid("origin_id must contain 1..=255 bytes"));
        }
        if self.dispatch_capacity == 0 {
            return Err(invalid("dispatch_capacity must be greater than zero"));
        }
        if self.max_samples_per_poll <= 0 {
            return Err(invalid("max_samples_per_poll must be greater than zero"));
        }
        if self.poll_interval.is_zero() {
            return Err(invalid("poll_interval must be greater than zero"));
        }
        if self.qos.history_depth == 0 {
            return Err(invalid("history_depth must be greater than zero"));
        }
        Ok(())
    }
}

/// One observable asynchronous transport error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthEvent {
    /// Monotonic sequence number within this health handle.
    pub sequence: u64,
    /// Stable error category.
    pub kind: HealthErrorKind,
    /// Human-readable detail.
    pub detail: String,
}

/// Categories reported by background transport work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthErrorKind {
    /// DDS reader or writer operation failed.
    Dds,
    /// A decoded outer sample violated its contract.
    MalformedSample,
    /// The bounded callback queue was full or disconnected.
    DispatchBackpressure,
    /// A callback panicked.
    CallbackPanic,
    /// A poller or dispatcher thread panicked during shutdown.
    WorkerPanic,
    /// DDS entity teardown failed.
    Teardown,
}

#[derive(Debug, Default)]
struct HealthState {
    next_sequence: u64,
    dds_errors: u64,
    malformed_samples: u64,
    dispatch_drops: u64,
    callback_panics: u64,
    worker_panics: u64,
    teardown_errors: u64,
    events: VecDeque<HealthEvent>,
}

/// Snapshot of transport health counters and recent errors.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HealthSnapshot {
    /// DDS operation failures.
    pub dds_errors: u64,
    /// Rejected decoded outer samples.
    pub malformed_samples: u64,
    /// Callback jobs rejected by bounded dispatch.
    pub dispatch_drops: u64,
    /// Callback panics caught by the dispatcher.
    pub callback_panics: u64,
    /// Poller or dispatcher join failures.
    pub worker_panics: u64,
    /// DDS teardown failures.
    pub teardown_errors: u64,
    /// At most the 64 most recent errors.
    pub recent_events: Vec<HealthEvent>,
}

/// Cloneable health handle that remains readable after the transport is dropped.
#[derive(Clone, Debug, Default)]
pub struct DdsHealth(Arc<Mutex<HealthState>>);

impl DdsHealth {
    /// Returns an atomic snapshot of all health information.
    #[must_use]
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = lock(&self.0);
        HealthSnapshot {
            dds_errors: state.dds_errors,
            malformed_samples: state.malformed_samples,
            dispatch_drops: state.dispatch_drops,
            callback_panics: state.callback_panics,
            worker_panics: state.worker_panics,
            teardown_errors: state.teardown_errors,
            recent_events: state.events.iter().cloned().collect(),
        }
    }

    pub(crate) fn record(&self, kind: HealthErrorKind, detail: impl Into<String>) {
        let mut state = lock(&self.0);
        match kind {
            HealthErrorKind::Dds => state.dds_errors += 1,
            HealthErrorKind::MalformedSample => state.malformed_samples += 1,
            HealthErrorKind::DispatchBackpressure => state.dispatch_drops += 1,
            HealthErrorKind::CallbackPanic => state.callback_panics += 1,
            HealthErrorKind::WorkerPanic => state.worker_panics += 1,
            HealthErrorKind::Teardown => state.teardown_errors += 1,
        }
        state.next_sequence += 1;
        let sequence = state.next_sequence;
        if state.events.len() == MAX_HEALTH_EVENTS {
            state.events.pop_front();
        }
        state.events.push_back(HealthEvent {
            sequence,
            kind,
            detail: detail.into(),
        });
    }
}

pub(crate) fn reader_qos(config: &DdsQos) -> DataReaderQos {
    let mut qos = DataReaderQos::default();
    qos.history.kind = HistoryQosPolicyKind::KeepLast(config.history_depth);
    qos.reliability.kind = reliability(config.reliability);
    qos
}

pub(crate) fn writer_qos(config: &DdsQos) -> DataWriterQos {
    let mut qos = DataWriterQos::default();
    qos.history.kind = HistoryQosPolicyKind::KeepLast(config.history_depth);
    qos.reliability.kind = reliability(config.reliability);
    qos
}

fn reliability(value: Reliability) -> ReliabilityQosPolicyKind {
    match value {
        Reliability::Reliable => ReliabilityQosPolicyKind::Reliable,
        Reliability::BestEffort => ReliabilityQosPolicyKind::BestEffort,
    }
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn dds_status(context: &str, error: DdsError) -> UStatus {
    let code = match error {
        DdsError::BadParameter | DdsError::InconsistentPolicy => UCode::InvalidArgument,
        DdsError::OutOfResources => UCode::ResourceExhausted,
        DdsError::Timeout => UCode::DeadlineExceeded,
        DdsError::NotEnabled | DdsError::AlreadyDeleted => UCode::Unavailable,
        _ => UCode::Internal,
    };
    UStatus::fail_with_code(code, format!("{context}: {error}"))
}

pub(crate) fn invalid(detail: impl Into<String>) -> UStatus {
    UStatus::fail_with_code(UCode::InvalidArgument, detail.into())
}

pub(crate) fn wait_until(
    mut current_matches: impl FnMut() -> Result<i32, DdsError>,
    required_matches: usize,
    timeout: Duration,
) -> Result<(), UStatus> {
    if required_matches == 0 {
        return Ok(());
    }
    let deadline = Instant::now() + timeout;
    loop {
        let count = current_matches().map_err(|error| dds_status("read readiness", error))?;
        if usize::try_from(count).unwrap_or_default() >= required_matches {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(UStatus::fail_with_code(
                UCode::DeadlineExceeded,
                format!(
                    "DDS discovery timed out with {count} matched readers; required {required_matches}"
                ),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Debug, Default)]
pub(crate) struct StopToken(Arc<(Mutex<bool>, Condvar)>);

impl Clone for StopToken {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl StopToken {
    pub(crate) fn stop(&self) {
        let (mutex, wake) = &*self.0;
        *lock(mutex) = true;
        wake.notify_all();
    }

    pub(crate) fn wait(&self, duration: Duration) -> bool {
        let (mutex, wake) = &*self.0;
        let stopped = lock(mutex);
        if *stopped {
            return true;
        }
        let (stopped, _) = wake
            .wait_timeout(stopped, duration)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *stopped
    }

    pub(crate) fn is_stopped(&self) -> bool {
        *lock(&self.0 .0)
    }
}

pub(crate) type Job = Box<dyn FnOnce() + Send + 'static>;

pub(crate) fn start_dispatcher(
    capacity: usize,
    health: DdsHealth,
) -> Result<(SyncSender<Job>, JoinHandle<()>), UStatus> {
    let (sender, receiver) = mpsc::sync_channel::<Job>(capacity);
    let thread = std::thread::Builder::new()
        .name("up-dds-dispatch".to_owned())
        .spawn(move || {
            while let Ok(job) = receiver.recv() {
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                    health.record(HealthErrorKind::CallbackPanic, "listener callback panicked");
                }
            }
        })
        .map_err(|error| {
            UStatus::fail_with_code(
                UCode::ResourceExhausted,
                format!("spawn callback dispatcher: {error}"),
            )
        })?;
    Ok((sender, thread))
}

pub(crate) fn run_callback<F>(
    runtime: &tokio::runtime::Handle,
    stop: &StopToken,
    health: &DdsHealth,
    future: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let task = runtime.spawn(future);
    while !task.is_finished() && !stop.wait(Duration::from_millis(10)) {}
    if stop.is_stopped() && !task.is_finished() {
        task.abort();
    }
    if runtime.block_on(task).is_err() && !stop.is_stopped() {
        health.record(HealthErrorKind::CallbackPanic, "listener callback panicked");
    }
}

pub(crate) fn submit(sender: &SyncSender<Job>, health: &DdsHealth, job: Job) {
    if let Err(error) = sender.try_send(job) {
        let detail = match error {
            TrySendError::Full(_) => "callback queue is full",
            TrySendError::Disconnected(_) => "callback queue is disconnected",
        };
        health.record(HealthErrorKind::DispatchBackpressure, detail);
    }
}

#[derive(Debug)]
struct RegistrationState {
    active: bool,
    in_flight: usize,
}

pub(crate) struct Registration<L: ?Sized> {
    source: UUri,
    sink: Option<UUri>,
    listener: Arc<L>,
    state: Mutex<RegistrationState>,
    idle: Condvar,
}

impl<L: ?Sized> Registration<L> {
    pub(crate) fn listener(&self) -> &Arc<L> {
        &self.listener
    }

    pub(crate) fn begin(self: &Arc<Self>) -> Option<InFlight<L>> {
        let mut state = lock(&self.state);
        if !state.active {
            return None;
        }
        state.in_flight += 1;
        drop(state);
        Some(InFlight(Arc::clone(self)))
    }

    fn deactivate_and_wait(&self) {
        let mut state = lock(&self.state);
        state.active = false;
        while state.in_flight != 0 {
            state = self
                .idle
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

pub(crate) struct InFlight<L: ?Sized>(Arc<Registration<L>>);

impl<L: ?Sized> Drop for InFlight<L> {
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.0.idle.notify_all();
        }
    }
}

pub(crate) struct Registry<L: ?Sized> {
    entries: RwLock<Vec<Arc<Registration<L>>>>,
}

impl<L: ?Sized> Default for Registry<L> {
    fn default() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
        }
    }
}

impl<L: ?Sized> Registry<L> {
    pub(crate) fn register(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        listener: Arc<L>,
    ) -> Result<(), UStatus> {
        let mut entries = write(&self.entries);
        if entries.iter().any(|entry| {
            entry.source == *source
                && entry.sink.as_ref() == sink
                && Arc::ptr_eq(&entry.listener, &listener)
        }) {
            return Err(UStatus::fail_with_code(
                UCode::AlreadyExists,
                "listener is already registered",
            ));
        }
        entries.push(Arc::new(Registration {
            source: source.clone(),
            sink: sink.cloned(),
            listener,
            state: Mutex::new(RegistrationState {
                active: true,
                in_flight: 0,
            }),
            idle: Condvar::new(),
        }));
        Ok(())
    }

    pub(crate) fn unregister(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        listener: &Arc<L>,
    ) -> Result<(), UStatus> {
        let registration = {
            let mut entries = write(&self.entries);
            let Some(index) = entries.iter().position(|entry| {
                entry.source == *source
                    && entry.sink.as_ref() == sink
                    && Arc::ptr_eq(&entry.listener, listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NotFound,
                    "listener is not registered",
                ));
            };
            entries.remove(index)
        };
        registration.deactivate_and_wait();
        Ok(())
    }

    pub(crate) fn matching(&self, source: &UUri, sink: Option<&UUri>) -> Vec<Arc<Registration<L>>> {
        read(&self.entries)
            .iter()
            .filter(|entry| {
                entry.source.matches(source)
                    && match (&entry.sink, sink) {
                        (None, None) => true,
                        (Some(filter), Some(value)) => filter.matches(value),
                        _ => false,
                    }
            })
            .cloned()
            .collect()
    }

    pub(crate) fn deactivate_all(&self) {
        let entries = {
            let mut entries = write(&self.entries);
            std::mem::take(&mut *entries)
        };
        for entry in entries {
            entry.deactivate_and_wait();
        }
    }
}

pub(crate) fn join_worker(worker: Option<JoinHandle<()>>, name: &str, health: &DdsHealth) {
    if worker.is_some_and(|thread| thread.join().is_err()) {
        health.record(
            HealthErrorKind::WorkerPanic,
            format!("{name} thread panicked"),
        );
    }
}

/// Heap allocation whose visible byte range honors an arbitrary valid alignment.
pub(crate) struct AlignedBytes {
    pointer: NonNull<u8>,
    layout: Layout,
    len: usize,
}

// SAFETY: the allocation is uniquely owned and exposes only slices governed by
// Rust's borrowing rules. Moving the owner does not move its allocation.
unsafe impl Send for AlignedBytes {}

impl AlignedBytes {
    pub(crate) fn zeroed(len: usize, alignment: usize) -> Result<Self, UStatus> {
        Self::allocate(len, alignment, true)
    }

    pub(crate) fn uninitialized(len: usize, alignment: usize) -> Result<Self, UStatus> {
        Self::allocate(len, alignment, false)
    }

    fn allocate(len: usize, alignment: usize, zeroed: bool) -> Result<Self, UStatus> {
        let layout = Layout::from_size_align(len.max(1), alignment)
            .map_err(|_| invalid("invalid requested payload allocation layout"))?;
        // SAFETY: `layout` is valid and the returned pointer is checked.
        let raw = unsafe {
            if zeroed {
                alloc_zeroed(layout)
            } else {
                alloc(layout)
            }
        };
        let pointer = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        Ok(Self {
            pointer,
            layout,
            len,
        })
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: the initialized constructor or the up-rust witness guarantees
        // initialization before this method is reachable on an initialized loan.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.len) }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: this owner has exclusive access to the allocated visible range.
        unsafe { std::slice::from_raw_parts_mut(self.pointer.as_ptr(), self.len) }
    }

    pub(crate) fn as_uninit_mut_slice(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: `MaybeUninit<u8>` has the same layout as `u8`, and this owner
        // has exclusive access to the allocated visible range.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.pointer.as_ptr().cast::<MaybeUninit<u8>>(),
                self.len,
            )
        }
    }
}

impl Drop for AlignedBytes {
    fn drop(&mut self) {
        // SAFETY: `pointer` was allocated with this exact layout and is owned here.
        unsafe { dealloc(self.pointer.as_ptr(), self.layout) };
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn read<T>(rwlock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    rwlock
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write<T>(rwlock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    rwlock
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
