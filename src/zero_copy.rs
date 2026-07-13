// SPDX-License-Identifier: Apache-2.0
//! Behavioral zero-copy family over Dust DDS.
//!
//! Transmit loans are exclusive transport-owned aligned heap allocations.
//! Dust DDS 0.15 provides no native transmit loan, receive loan, or shared
//! memory API, so commit and receive necessarily copy. No end-to-end no-copy
//! claim is made.

use std::mem::MaybeUninit;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use dust_dds::domain::domain_participant::DomainParticipant;
use dust_dds::domain::domain_participant_factory::DomainParticipantFactory;
use dust_dds::infrastructure::error::DdsError;
use dust_dds::infrastructure::qos::QosKind;
use dust_dds::infrastructure::sample_info::{ANY_INSTANCE_STATE, ANY_SAMPLE_STATE, ANY_VIEW_STATE};
use dust_dds::infrastructure::status::NO_STATUS;
use dust_dds::infrastructure::type_support::DdsType;
use dust_dds::listener::NO_LISTENER;
use dust_dds::publication::data_writer::DataWriter;
use up_rust::transport_implementer_api::{
    PreparedTxLoanSpec, UEncodedRxFrame, UEncodedZeroCopyListener, UZeroCopyTransportCore,
    UZeroCopyUninitTransportCore,
};
use up_rust::{UCode, UFrameMetadata, UStatus, UTxBuffer, UUninitTxBuffer, UUri};

use crate::runtime::{
    complete_on_drop, complete_send, dds_status, join_worker, reader_qos, run_callback,
    start_dispatcher, submit, tracks_delivery, wait_until, writer_qos, AlignedBytes,
    DeliveryTracker, HealthErrorKind, Registry, StopToken,
};
use crate::{DdsConfig, DdsHealth};

/// Versioned behavioral zero-copy DDS topic.
pub const ZERO_COPY_TOPIC_V1: &str = "uprotocol.zero-copy.v1";
/// Versioned behavioral zero-copy DDS registered type name.
pub const ZERO_COPY_TYPE_V1: &str = "UpDdsZeroCopySampleV1";

/// Normative behavioral zero-copy outer sample.
#[derive(Clone, Debug, DdsType)]
pub struct UpDdsZeroCopySampleV1 {
    /// Exact originating transport instance.
    pub origin_id: String,
    /// Side-band source URI used only for routing.
    pub source_uri: String,
    /// Whether the side-band sink URI is present.
    pub has_sink: bool,
    /// Side-band sink URI, empty only when `has_sink` is false.
    pub sink_uri: String,
    /// Whether a payload is present, including a present empty payload.
    pub has_payload: bool,
    /// Opaque selected-wire metadata, carried byte-for-byte.
    pub encoded_metadata: Vec<u8>,
    /// Opaque selected-wire payload bytes.
    pub payload: Vec<u8>,
}

/// Initialized aligned transmit loan.
pub struct DdsTxBuffer {
    metadata: UFrameMetadata,
    source_uri: String,
    sink_uri: Option<String>,
    has_payload: bool,
    encoded_metadata: Vec<u8>,
    payload: AlignedBytes,
}

impl UTxBuffer for DdsTxBuffer {
    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        self.payload.as_mut_slice()
    }
}

/// Uninitialized aligned transmit loan.
pub struct DdsUninitTxBuffer {
    metadata: UFrameMetadata,
    source_uri: String,
    sink_uri: Option<String>,
    has_payload: bool,
    encoded_metadata: Vec<u8>,
    payload: AlignedBytes,
}

impl UUninitTxBuffer for DdsUninitTxBuffer {
    type Initialized = DdsTxBuffer;

    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload_len(&self) -> usize {
        self.payload.len()
    }

    fn payload_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.payload.as_uninit_mut_slice()
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        DdsTxBuffer {
            metadata: self.metadata,
            source_uri: self.source_uri,
            sink_uri: self.sink_uri,
            has_payload: self.has_payload,
            encoded_metadata: self.encoded_metadata,
            payload: self.payload,
        }
    }
}

/// Immutable receive lease backed by owned taken-sample storage.
#[derive(Clone, Debug)]
pub struct DdsRxFrame {
    encoded_metadata: Arc<[u8]>,
    payload: Arc<[u8]>,
}

impl UEncodedRxFrame for DdsRxFrame {
    type PayloadReader<'a>
        = &'a [u8]
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn encoded_metadata(&self) -> &[u8] {
        &self.encoded_metadata
    }

    fn payload_len(&self) -> usize {
        self.payload.len()
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        &self.payload
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(&self.payload)
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        Some(&self.payload)
    }
}

type EncodedListener = dyn UEncodedZeroCopyListener<DdsRxFrame>;

/// Behavioral zero-copy core over a versioned Dust DDS topic.
pub struct DdsZeroCopyCore {
    config: DdsConfig,
    writer: Option<DataWriter<UpDdsZeroCopySampleV1>>,
    participant: Option<DomainParticipant>,
    registrations: Arc<Registry<EncodedListener>>,
    delivery: DeliveryTracker,
    health: DdsHealth,
    stop: StopToken,
    poller: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl DdsZeroCopyCore {
    /// Creates a zero-copy core with default configuration for `domain_id`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or failed DDS entity/worker creation.
    pub fn new(domain_id: i32, runtime: tokio::runtime::Handle) -> Result<Self, UStatus> {
        Self::with_config(DdsConfig::new(domain_id), runtime)
    }

    /// Creates a zero-copy core with explicit domain, origin, `QoS`, and limits.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or failed DDS entity/worker creation.
    #[allow(clippy::too_many_lines)]
    pub fn with_config(
        config: DdsConfig,
        runtime: tokio::runtime::Handle,
    ) -> Result<Self, UStatus> {
        config.validate()?;
        let factory = DomainParticipantFactory::get_instance();
        let participant = factory
            .create_participant(config.domain_id, QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create zero-copy participant", error))?;
        let topic = participant
            .create_topic::<UpDdsZeroCopySampleV1>(
                ZERO_COPY_TOPIC_V1,
                ZERO_COPY_TYPE_V1,
                QosKind::Default,
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create zero-copy topic", error))?;
        let publisher = participant
            .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create zero-copy publisher", error))?;
        let writer = publisher
            .create_datawriter::<UpDdsZeroCopySampleV1>(
                &topic,
                QosKind::Specific(writer_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create zero-copy writer", error))?;
        let subscriber = participant
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create zero-copy subscriber", error))?;
        let reader = subscriber
            .create_datareader::<UpDdsZeroCopySampleV1>(
                &topic,
                QosKind::Specific(reader_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create zero-copy reader", error))?;

        let registrations = Arc::<Registry<EncodedListener>>::default();
        let delivery = DeliveryTracker::default();
        let health = DdsHealth::default();
        let stop = StopToken::default();
        let (dispatch, dispatcher) = start_dispatcher(config.dispatch_capacity, health.clone())?;
        let poll_registrations = Arc::clone(&registrations);
        let poll_delivery = delivery.clone();
        let poll_health = health.clone();
        let poll_stop = stop.clone();
        let callback_stop = stop.clone();
        let poll_config = config.clone();
        let poller = std::thread::Builder::new()
            .name("up-dds-zero-copy-poll".to_owned())
            .spawn(move || {
                while !poll_stop.wait(poll_config.poll_interval) {
                    match reader.take(
                        poll_config.max_samples_per_poll,
                        ANY_SAMPLE_STATE,
                        ANY_VIEW_STATE,
                        ANY_INSTANCE_STATE,
                    ) {
                        Ok(samples) => {
                            for sample in samples {
                                let Some(sample) = sample.data else {
                                    continue;
                                };
                                if sample.origin_id == poll_config.origin_id {
                                    poll_delivery.record_delivery();
                                    continue;
                                }
                                let (source, sink, frame) = match decode_zero_copy(sample) {
                                    Ok(frame) => frame,
                                    Err(detail) => {
                                        poll_health
                                            .record(HealthErrorKind::MalformedSample, detail);
                                        continue;
                                    }
                                };
                                for registration in
                                    poll_registrations.matching(&source, sink.as_ref())
                                {
                                    let frame = frame.clone();
                                    let runtime = runtime.clone();
                                    let callback_stop = callback_stop.clone();
                                    let callback_health = poll_health.clone();
                                    submit(
                                        &dispatch,
                                        &poll_health,
                                        Box::new(move || {
                                            let Some(in_flight) = registration.begin() else {
                                                return;
                                            };
                                            let listener = Arc::clone(registration.listener());
                                            run_callback(
                                                &runtime,
                                                &callback_stop,
                                                &callback_health,
                                                async move {
                                                    let _in_flight = in_flight;
                                                    listener
                                                        .on_receive_encoded_zero_copy(frame)
                                                        .await;
                                                },
                                            );
                                        }),
                                    );
                                }
                            }
                        }
                        Err(DdsError::NoData) => {}
                        Err(error) => poll_health.record(
                            HealthErrorKind::Dds,
                            format!("zero-copy reader take: {error}"),
                        ),
                    }
                }
            })
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::ResourceExhausted,
                    format!("spawn zero-copy poller: {error}"),
                )
            })?;

        Ok(Self {
            config,
            writer: Some(writer),
            participant: Some(participant),
            registrations,
            delivery,
            health,
            stop,
            poller: Some(poller),
            dispatcher: Some(dispatcher),
        })
    }

    /// Waits for a deterministic DDS publication-match count.
    ///
    /// # Errors
    ///
    /// Returns an error if DDS status cannot be read or the timeout expires.
    pub fn wait_ready(
        &self,
        required_matched_readers: usize,
        timeout: Duration,
    ) -> Result<(), UStatus> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        wait_until(
            || {
                writer
                    .get_publication_matched_status()
                    .map(|status| status.current_count)
            },
            required_matched_readers,
            timeout,
        )
    }

    /// Returns a health handle that remains valid after shutdown.
    #[must_use]
    pub fn health(&self) -> DdsHealth {
        self.health.clone()
    }

    /// Returns this instance's configured origin identity.
    #[must_use]
    pub fn origin_id(&self) -> &str {
        &self.config.origin_id
    }
}

fn decode_zero_copy(
    sample: UpDdsZeroCopySampleV1,
) -> Result<(UUri, Option<UUri>, DdsRxFrame), String> {
    if !sample.has_sink && !sample.sink_uri.is_empty() {
        return Err("zero-copy sample has sink bytes while has_sink is false".to_owned());
    }
    if sample.has_sink && sample.sink_uri.is_empty() {
        return Err("zero-copy sample has an empty present sink".to_owned());
    }
    if !sample.has_payload && !sample.payload.is_empty() {
        return Err("zero-copy sample has payload bytes while has_payload is false".to_owned());
    }
    let source = sample
        .source_uri
        .parse::<UUri>()
        .map_err(|error| format!("invalid zero-copy source URI: {error}"))?;
    let sink = sample
        .has_sink
        .then(|| sample.sink_uri.parse::<UUri>())
        .transpose()
        .map_err(|error| format!("invalid zero-copy sink URI: {error}"))?;
    let frame = DdsRxFrame {
        encoded_metadata: Arc::from(sample.encoded_metadata),
        payload: Arc::from(sample.payload),
    };
    Ok((source, sink, frame))
}

fn loan_parts(
    spec: &PreparedTxLoanSpec,
) -> (UFrameMetadata, String, Option<String>, bool, Vec<u8>) {
    (
        spec.metadata().clone(),
        spec.metadata().source().to_uri(false),
        spec.metadata().sink().map(|sink| sink.to_uri(false)),
        spec.has_payload(),
        spec.encoded_metadata().to_vec(),
    )
}

#[async_trait]
impl UZeroCopyTransportCore for DdsZeroCopyCore {
    type Tx = DdsTxBuffer;
    type Rx = DdsRxFrame;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let payload = AlignedBytes::zeroed(spec.payload_len(), spec.payload_alignment())?;
        let (metadata, source_uri, sink_uri, has_payload, encoded_metadata) = loan_parts(&spec);
        Ok(DdsTxBuffer {
            metadata,
            source_uri,
            sink_uri,
            has_payload,
            encoded_metadata,
            payload,
        })
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        let _write_guard = tracks_delivery(&self.config).then(|| self.delivery.write_guard());
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        let (has_sink, sink_uri) = buffer
            .sink_uri
            .map_or((false, String::new()), |sink| (true, sink));
        writer
            .write(
                UpDdsZeroCopySampleV1 {
                    origin_id: self.config.origin_id.clone(),
                    source_uri: buffer.source_uri,
                    has_sink,
                    sink_uri,
                    has_payload: buffer.has_payload,
                    encoded_metadata: buffer.encoded_metadata,
                    payload: buffer.payload.as_slice().to_vec(),
                },
                None,
            )
            .map_err(|error| {
                self.health
                    .record(HealthErrorKind::Dds, format!("zero-copy writer: {error}"));
                dds_status("write zero-copy sample", error)
            })?;
        complete_send(
            writer,
            &self.config,
            &self.health,
            &self.delivery,
            "acknowledge zero-copy sample",
        )
    }

    async fn register_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        self.registrations
            .register(source_filter, sink_filter, listener)
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        self.registrations
            .unregister(source_filter, sink_filter, &listener)
    }
}

#[async_trait]
impl UZeroCopyUninitTransportCore for DdsZeroCopyCore {
    type UninitTx = DdsUninitTxBuffer;

    async fn loan_prepared_uninit_tx(
        &self,
        spec: PreparedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let payload = AlignedBytes::uninitialized(spec.payload_len(), spec.payload_alignment())?;
        let (metadata, source_uri, sink_uri, has_payload, encoded_metadata) = loan_parts(&spec);
        Ok(DdsUninitTxBuffer {
            metadata,
            source_uri,
            sink_uri,
            has_payload,
            encoded_metadata,
            payload,
        })
    }
}

impl Drop for DdsZeroCopyCore {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_ref() {
            complete_on_drop(
                writer,
                &self.config,
                &self.health,
                &self.delivery,
                "acknowledge zero-copy samples during teardown",
            );
        }
        self.stop.stop();
        join_worker(self.poller.take(), "zero-copy poller", &self.health);
        self.registrations.deactivate_all();
        join_worker(self.dispatcher.take(), "zero-copy dispatcher", &self.health);
        drop(self.writer.take());
        if let Some(participant) = self.participant.take() {
            if let Err(error) = participant.delete_contained_entities() {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete zero-copy contained entities: {error}"),
                );
            }
            if let Err(error) =
                DomainParticipantFactory::get_instance().delete_participant(&participant)
            {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete zero-copy participant: {error}"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_copy_rejects_inconsistent_sideband_presence() {
        let error = decode_zero_copy(UpDdsZeroCopySampleV1 {
            origin_id: "peer".to_owned(),
            source_uri: "up://peer/10000/1/8001".to_owned(),
            has_sink: false,
            sink_uri: "up://unexpected/10000/1/0".to_owned(),
            has_payload: false,
            encoded_metadata: Vec::new(),
            payload: Vec::new(),
        })
        .expect_err("inconsistent sample must fail");
        assert!(error.contains("has_sink"));
    }
}
