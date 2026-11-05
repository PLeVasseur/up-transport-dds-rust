// SPDX-License-Identifier: Apache-2.0
//! Selected-wire encoded-owned frames over owned Dust DDS samples.
//!
//! Dust DDS 0.16 accepts owned samples and returns owned taken samples; it exposes
//! no native TX/RX loan or shared-memory data-sharing API. Serialization and
//! deserialization still copy. This core implements only the owned transport
//! family and delegates selected-wire validation to the SDK adapter.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use dust_dds::domain::domain_participant::DomainParticipant;
use dust_dds::domain::domain_participant_factory::DomainParticipantFactory;
use dust_dds::infrastructure::error::DdsError;
use dust_dds::infrastructure::listener::NO_LISTENER;
use dust_dds::infrastructure::qos::QosKind;
use dust_dds::infrastructure::sample_info::{ANY_INSTANCE_STATE, ANY_SAMPLE_STATE, ANY_VIEW_STATE};
use dust_dds::infrastructure::status::NO_STATUS;
use dust_dds::infrastructure::type_support::DdsType;
use dust_dds::publication::data_writer::DataWriter;
use up_rust::{
    EncodedOwnedFrame, PreparedOwnedFrame, UCode, UEncodedOwnedListener, UOwnedTransportCore,
    UStatus, UUri,
};

use crate::runtime::{
    dds_status, join_worker, reader_qos, run_callback, start_dispatcher, submit, wait_until,
    writer_qos, HealthErrorKind, Registry, StopToken,
};
use crate::{DdsConfig, DdsHealth};

/// Versioned selected-wire encoded-owned DDS topic, distinct from validated owned carriage.
pub const ENCODED_OWNED_TOPIC_V1: &str = "uprotocol.encoded-owned.v1";
/// Versioned selected-wire encoded-owned DDS registered type name.
pub const ENCODED_OWNED_TYPE_V1: &str = "UpDdsEncodedOwnedSampleV1";

/// Selected-wire encoded-owned outer sample with untrusted sideband routing hints.
#[derive(Clone, Debug, DdsType)]
#[dust_dds(extensibility = "final")]
pub struct UpDdsEncodedOwnedSampleV1 {
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

type EncodedListener = dyn UEncodedOwnedListener;

/// Selected-wire encoded-owned core over a distinct DDS topic and owned samples.
///
/// Transmit payloads are copied into the middleware's owned sample representation;
/// taken payload vectors are adopted into owned bytes. No native DDS loan or
/// zero-copy transport trait is exposed. Compose with the SDK's selected-wire
/// adapter for validated metadata, identity agreement and final listener filtering.
pub struct DdsEncodedOwnedCore {
    config: DdsConfig,
    writer: Option<DataWriter<UpDdsEncodedOwnedSampleV1>>,
    participant: Option<DomainParticipant>,
    registrations: Arc<Registry<EncodedListener>>,
    health: DdsHealth,
    stop: StopToken,
    poller: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl DdsEncodedOwnedCore {
    /// Creates an encoded-owned core with default configuration for `domain_id`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or failed DDS entity/worker creation.
    pub fn new(domain_id: i32, runtime: tokio::runtime::Handle) -> Result<Self, UStatus> {
        Self::with_config(DdsConfig::new(domain_id), runtime)
    }

    /// Creates an encoded-owned core with explicit domain, origin, `QoS`, and limits.
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
            .map_err(|error| dds_status("create encoded-owned participant", error))?;
        let topic = participant
            .create_topic::<UpDdsEncodedOwnedSampleV1>(
                ENCODED_OWNED_TOPIC_V1,
                ENCODED_OWNED_TYPE_V1,
                QosKind::Default,
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create encoded-owned topic", error))?;
        let publisher = participant
            .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create encoded-owned publisher", error))?;
        let writer = publisher
            .create_datawriter::<UpDdsEncodedOwnedSampleV1>(
                &topic,
                QosKind::Specific(writer_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create encoded-owned writer", error))?;
        let subscriber = participant
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create encoded-owned subscriber", error))?;
        let reader = subscriber
            .create_datareader::<UpDdsEncodedOwnedSampleV1>(
                &topic,
                QosKind::Specific(reader_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create encoded-owned reader", error))?;

        let registrations = Arc::<Registry<EncodedListener>>::default();
        let health = DdsHealth::default();
        let stop = StopToken::default();
        let (dispatch, dispatcher) = start_dispatcher(config.dispatch_capacity, health.clone())?;
        let poll_registrations = Arc::clone(&registrations);
        let poll_health = health.clone();
        let poll_stop = stop.clone();
        let callback_stop = stop.clone();
        let poll_config = config.clone();
        let poller = std::thread::Builder::new()
            .name("up-dds-encoded-owned-poll".to_owned())
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
                                    continue;
                                }
                                let (source, sink, frame) = match decode_encoded_owned(sample) {
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
                                                in_flight.enclose(async move {
                                                    listener.on_receive_encoded_owned(frame).await;
                                                }),
                                            );
                                        }),
                                    );
                                }
                            }
                        }
                        Err(DdsError::NoData) => {}
                        Err(error) => poll_health.record(
                            HealthErrorKind::Dds,
                            format!("encoded-owned reader take: {error}"),
                        ),
                    }
                }
            })
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::ResourceExhausted,
                    format!("spawn encoded-owned poller: {error}"),
                )
            })?;

        Ok(Self {
            config,
            writer: Some(writer),
            participant: Some(participant),
            registrations,
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

    /// Waits for prior writes to be acknowledged by matched reliable readers.
    ///
    /// Call after discovery and terminal sends before dropping the producer.
    /// This does not wait for future readers or application-level processing.
    ///
    /// # Errors
    ///
    /// Returns an error for an unrepresentable timeout, a DDS failure or timeout.
    pub fn wait_acknowledged(&self, timeout: Duration) -> Result<(), UStatus> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        crate::runtime::wait_acknowledged(writer, timeout)
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

fn decode_encoded_owned(
    sample: UpDdsEncodedOwnedSampleV1,
) -> Result<(UUri, Option<UUri>, EncodedOwnedFrame), String> {
    if !sample.has_sink && !sample.sink_uri.is_empty() {
        return Err("encoded-owned sample has sink bytes while has_sink is false".to_owned());
    }
    if sample.has_sink && sample.sink_uri.is_empty() {
        return Err("encoded-owned sample has an empty present sink".to_owned());
    }
    if !sample.has_payload && !sample.payload.is_empty() {
        return Err("encoded-owned sample has payload bytes while has_payload is false".to_owned());
    }
    let source = sample
        .source_uri
        .parse::<UUri>()
        .map_err(|error| format!("invalid encoded-owned source URI: {error}"))?;
    let sink = sample
        .has_sink
        .then(|| sample.sink_uri.parse::<UUri>())
        .transpose()
        .map_err(|error| format!("invalid encoded-owned sink URI: {error}"))?;
    let frame = EncodedOwnedFrame::new(
        sample.encoded_metadata,
        sample.has_payload.then(|| Bytes::from(sample.payload)),
    );
    Ok((source, sink, frame))
}

#[async_trait]
impl UOwnedTransportCore for DdsEncodedOwnedCore {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        let (metadata, encoded_metadata, payload) = frame.into_parts();
        let source_uri = metadata.source().to_uri(false);
        let (has_sink, sink_uri) = metadata
            .sink()
            .map_or((false, String::new()), |sink| (true, sink.to_uri(false)));
        writer
            .write(
                UpDdsEncodedOwnedSampleV1 {
                    origin_id: self.config.origin_id.clone(),
                    source_uri,
                    has_sink,
                    sink_uri,
                    has_payload: payload.is_some(),
                    encoded_metadata,
                    payload: payload.map_or_else(Vec::new, |bytes| bytes.to_vec()),
                },
                None,
            )
            .map_err(|error| {
                self.health.record(
                    HealthErrorKind::Dds,
                    format!("encoded-owned writer: {error}"),
                );
                dds_status("write encoded-owned sample", error)
            })
    }

    async fn register_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        self.registrations
            .register(source_filter, sink_filter, listener)
    }

    async fn unregister_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        self.registrations
            .unregister(source_filter, sink_filter, &listener)
    }
}

impl Drop for DdsEncodedOwnedCore {
    fn drop(&mut self) {
        self.stop.stop();
        join_worker(self.poller.take(), "encoded-owned poller", &self.health);
        self.registrations.deactivate_all();
        join_worker(
            self.dispatcher.take(),
            "encoded-owned dispatcher",
            &self.health,
        );
        drop(self.writer.take());
        if let Some(participant) = self.participant.take() {
            if let Err(error) = participant.delete_contained_entities() {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete encoded-owned contained entities: {error}"),
                );
            }
            if let Err(error) =
                DomainParticipantFactory::get_instance().delete_participant(&participant)
            {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete encoded-owned participant: {error}"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taken_sample_allocations_are_reused_and_shared_without_an_extra_copy() {
        let metadata = vec![1, 2, 3, 4];
        let payload = b"owned DDS sample".to_vec();
        let metadata_address = metadata.as_ptr();
        let payload_address = payload.as_ptr();
        let (_, _, frame) = decode_encoded_owned(UpDdsEncodedOwnedSampleV1 {
            origin_id: "peer".to_owned(),
            source_uri: "up://peer/0/1/1/8001".to_owned(),
            has_sink: false,
            sink_uri: String::new(),
            has_payload: true,
            encoded_metadata: metadata,
            payload,
        })
        .unwrap();
        assert_eq!(frame.encoded_metadata().as_ptr(), metadata_address);
        assert_eq!(frame.payload().unwrap().as_ptr(), payload_address);
        let retained = frame.clone();
        drop(frame);
        assert_eq!(retained.encoded_metadata(), &[1, 2, 3, 4]);
        assert_eq!(retained.payload().unwrap().as_ptr(), payload_address);
        assert_eq!(retained.payload().unwrap().as_ref(), b"owned DDS sample");
    }

    #[test]
    fn encoded_owned_rejects_inconsistent_sideband_presence() {
        let error = decode_encoded_owned(UpDdsEncodedOwnedSampleV1 {
            origin_id: "peer".to_owned(),
            source_uri: "up://peer/0/1/1/8001".to_owned(),
            has_sink: false,
            sink_uri: "up://unexpected/0/1/1/0".to_owned(),
            has_payload: false,
            encoded_metadata: Vec::new(),
            payload: Vec::new(),
        })
        .expect_err("inconsistent sample must fail");
        assert!(error.contains("has_sink"));
    }
}
