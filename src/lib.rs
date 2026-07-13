// SPDX-License-Identifier: Apache-2.0
//! Dust DDS RTPS transports for Eclipse uProtocol.
//!
//! The crate implements three independently versioned carriage families:
//! classic [`UPTransportDds`], owned [`owned::UPTransportDdsOwned`], and the
//! behavioral zero-copy core [`zero_copy::DdsZeroCopyCore`]. See
//! `docs/wire-contract.md` for the exact claim boundary.

#![allow(clippy::module_name_repetitions)]

pub mod owned;
mod runtime;
pub mod zero_copy;

pub use runtime::{
    AcknowledgmentMode, DdsConfig, DdsHealth, DdsQos, HealthErrorKind, HealthEvent, HealthSnapshot,
    Reliability,
};

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
use up_rust::{
    try_project_attributes_to_frame_metadata, try_project_frame_to_umessage,
    verify_filter_criteria, ProtobufMappable as _, UAttributes, UCode, UListener, UMessage,
    UStatus, UTransport, UUri,
};

use crate::runtime::{
    complete_on_drop, complete_send, dds_status, join_worker, reader_qos, run_callback,
    start_dispatcher, submit, tracks_delivery, wait_until, writer_qos, DeliveryTracker,
    HealthErrorKind as ErrorKind, Registry, StopToken,
};

/// Versioned classic-family DDS topic.
pub const CLASSIC_TOPIC_V1: &str = "uprotocol.classic.v1";
/// Versioned classic-family DDS registered type name.
pub const CLASSIC_TYPE_V1: &str = "UpDdsClassicSampleV1";

/// Normative classic-family outer sample.
#[derive(Clone, Debug, DdsType)]
pub struct UpDdsClassicSampleV1 {
    /// Exact originating transport instance.
    pub origin_id: String,
    /// Canonical protobuf `UAttributes` bytes.
    pub attributes_proto: Vec<u8>,
    /// Whether a payload is present, including a present empty payload.
    pub has_payload: bool,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

/// Classic uProtocol transport over a versioned Dust DDS topic.
pub struct UPTransportDds {
    config: DdsConfig,
    writer: Option<DataWriter<UpDdsClassicSampleV1>>,
    participant: Option<DomainParticipant>,
    registrations: Arc<Registry<dyn UListener>>,
    delivery: DeliveryTracker,
    health: DdsHealth,
    stop: StopToken,
    poller: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl UPTransportDds {
    /// Creates a classic transport with default configuration for `domain_id`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or failed DDS entity/worker creation.
    pub fn new(domain_id: i32, runtime: tokio::runtime::Handle) -> Result<Self, UStatus> {
        Self::with_config(DdsConfig::new(domain_id), runtime)
    }

    /// Creates a classic transport with explicit domain, origin, `QoS`, and limits.
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
            .map_err(|error| dds_status("create classic participant", error))?;
        let topic = participant
            .create_topic::<UpDdsClassicSampleV1>(
                CLASSIC_TOPIC_V1,
                CLASSIC_TYPE_V1,
                QosKind::Default,
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create classic topic", error))?;
        let publisher = participant
            .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create classic publisher", error))?;
        let writer = publisher
            .create_datawriter::<UpDdsClassicSampleV1>(
                &topic,
                QosKind::Specific(writer_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create classic writer", error))?;
        let subscriber = participant
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create classic subscriber", error))?;
        let reader = subscriber
            .create_datareader::<UpDdsClassicSampleV1>(
                &topic,
                QosKind::Specific(reader_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create classic reader", error))?;

        let registrations = Arc::<Registry<dyn UListener>>::default();
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
            .name("up-dds-classic-poll".to_owned())
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
                                let message = match decode_classic(sample) {
                                    Ok(message) => message,
                                    Err(detail) => {
                                        poll_health.record(ErrorKind::MalformedSample, detail);
                                        continue;
                                    }
                                };
                                for registration in
                                    poll_registrations.matching(message.source(), message.sink())
                                {
                                    let message = message.clone();
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
                                                    listener.on_receive(message).await;
                                                },
                                            );
                                        }),
                                    );
                                }
                            }
                        }
                        Err(DdsError::NoData) => {}
                        Err(error) => poll_health
                            .record(ErrorKind::Dds, format!("classic reader take: {error}")),
                    }
                }
            })
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::ResourceExhausted,
                    format!("spawn classic poller: {error}"),
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

    /// Waits until DDS reports at least `required_matched_readers` compatible readers.
    ///
    /// A transport's own reader counts as one match. Cross-instance callers
    /// should therefore require at least two readers.
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

fn decode_classic(sample: UpDdsClassicSampleV1) -> Result<UMessage, String> {
    if !sample.has_payload && !sample.payload.is_empty() {
        return Err("classic sample has payload bytes while has_payload is false".to_owned());
    }
    let attributes = UAttributes::parse_from_protobuf_bytes(&sample.attributes_proto)
        .map_err(|error| format!("invalid classic attributes: {error}"))?;
    let metadata = try_project_attributes_to_frame_metadata(&attributes, None)
        .map_err(|error| format!("invalid classic metadata: {error}"))?;
    let payload = sample
        .has_payload
        .then(|| bytes::Bytes::from(sample.payload));
    try_project_frame_to_umessage(metadata, payload)
        .map_err(|error| format!("invalid classic message: {error}"))
}

#[async_trait]
impl UTransport for UPTransportDds {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let attributes_proto = message
            .attributes()
            .write_to_protobuf_bytes()
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("encode classic attributes: {error}"),
                )
            })?;
        let (has_payload, payload) = message
            .payload()
            .map_or((false, Vec::new()), |payload| (true, payload.to_vec()));
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        let _write_guard = tracks_delivery(&self.config).then(|| self.delivery.write_guard());
        writer
            .write(
                UpDdsClassicSampleV1 {
                    origin_id: self.config.origin_id.clone(),
                    attributes_proto,
                    has_payload,
                    payload,
                },
                None,
            )
            .map_err(|error| {
                self.health
                    .record(ErrorKind::Dds, format!("classic writer: {error}"));
                dds_status("write classic sample", error)
            })?;
        complete_send(
            writer,
            &self.config,
            &self.health,
            &self.delivery,
            "acknowledge classic sample",
        )
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter).map_err(|error| *error)?;
        self.registrations
            .register(source_filter, sink_filter, listener)
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter).map_err(|error| *error)?;
        self.registrations
            .unregister(source_filter, sink_filter, &listener)
    }
}

impl Drop for UPTransportDds {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_ref() {
            complete_on_drop(
                writer,
                &self.config,
                &self.health,
                &self.delivery,
                "acknowledge classic samples during teardown",
            );
        }
        self.stop.stop();
        join_worker(self.poller.take(), "classic poller", &self.health);
        self.registrations.deactivate_all();
        join_worker(self.dispatcher.take(), "classic dispatcher", &self.health);
        drop(self.writer.take());
        if let Some(participant) = self.participant.take() {
            if let Err(error) = participant.delete_contained_entities() {
                self.health.record(
                    ErrorKind::Teardown,
                    format!("delete classic contained entities: {error}"),
                );
            }
            if let Err(error) =
                DomainParticipantFactory::get_instance().delete_participant(&participant)
            {
                self.health.record(
                    ErrorKind::Teardown,
                    format!("delete classic participant: {error}"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_rejects_inconsistent_payload_presence() {
        let error = decode_classic(UpDdsClassicSampleV1 {
            origin_id: "peer".to_owned(),
            attributes_proto: Vec::new(),
            has_payload: false,
            payload: vec![1],
        })
        .expect_err("inconsistent sample must fail");
        assert!(error.contains("has_payload"));
    }
}
