// SPDX-License-Identifier: Apache-2.0
//! Owned validated-frame family over Dust DDS.

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
use up_rust::frame::codec::{decode_frame_metadata_fields, encode_frame_metadata_fields};
use up_rust::{
    UCode, UOwnedFrame, UOwnedListener, UOwnedTransportImpl, UStatus, UUri, ValidatedOwnedFrame,
};

use crate::runtime::{
    complete_on_drop, complete_send, dds_status, join_worker, reader_qos, run_callback,
    start_dispatcher, submit, tracks_delivery, wait_until, writer_qos, DeliveryTracker,
    HealthErrorKind, Registry, StopToken,
};
use crate::{DdsConfig, DdsHealth};

/// Versioned owned-family DDS topic.
pub const OWNED_TOPIC_V1: &str = "uprotocol.owned.v1";
/// Versioned owned-family DDS registered type name.
pub const OWNED_TYPE_V1: &str = "UpDdsOwnedSampleV1";

/// Normative owned-family outer sample.
#[derive(Clone, Debug, DdsType)]
pub struct UpDdsOwnedSampleV1 {
    /// Exact originating transport instance.
    pub origin_id: String,
    /// Canonical validated frame-metadata field block.
    pub metadata_fields: Vec<u8>,
    /// Whether a payload is present, including a present empty payload.
    pub has_payload: bool,
    /// Opaque payload bytes.
    pub payload: Vec<u8>,
}

/// Owned validated-frame transport over a versioned Dust DDS topic.
pub struct UPTransportDdsOwned {
    config: DdsConfig,
    writer: Option<DataWriter<UpDdsOwnedSampleV1>>,
    participant: Option<DomainParticipant>,
    registrations: Arc<Registry<dyn UOwnedListener>>,
    delivery: DeliveryTracker,
    health: DdsHealth,
    stop: StopToken,
    poller: Option<JoinHandle<()>>,
    dispatcher: Option<JoinHandle<()>>,
}

impl UPTransportDdsOwned {
    /// Creates an owned transport with default configuration for `domain_id`.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration or failed DDS entity/worker creation.
    pub fn new(domain_id: i32, runtime: tokio::runtime::Handle) -> Result<Self, UStatus> {
        Self::with_config(DdsConfig::new(domain_id), runtime)
    }

    /// Creates an owned transport with explicit domain, origin, `QoS`, and limits.
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
            .map_err(|error| dds_status("create owned participant", error))?;
        let topic = participant
            .create_topic::<UpDdsOwnedSampleV1>(
                OWNED_TOPIC_V1,
                OWNED_TYPE_V1,
                QosKind::Default,
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create owned topic", error))?;
        let publisher = participant
            .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create owned publisher", error))?;
        let writer = publisher
            .create_datawriter::<UpDdsOwnedSampleV1>(
                &topic,
                QosKind::Specific(writer_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create owned writer", error))?;
        let subscriber = participant
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .map_err(|error| dds_status("create owned subscriber", error))?;
        let reader = subscriber
            .create_datareader::<UpDdsOwnedSampleV1>(
                &topic,
                QosKind::Specific(reader_qos(&config.qos)),
                NO_LISTENER,
                NO_STATUS,
            )
            .map_err(|error| dds_status("create owned reader", error))?;

        let registrations = Arc::<Registry<dyn UOwnedListener>>::default();
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
            .name("up-dds-owned-poll".to_owned())
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
                                let frame = match decode_owned(sample) {
                                    Ok(frame) => frame,
                                    Err(detail) => {
                                        poll_health
                                            .record(HealthErrorKind::MalformedSample, detail);
                                        continue;
                                    }
                                };
                                for registration in poll_registrations
                                    .matching(frame.metadata().source(), frame.metadata().sink())
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
                                                    listener.on_receive_owned(frame).await;
                                                },
                                            );
                                        }),
                                    );
                                }
                            }
                        }
                        Err(DdsError::NoData) => {}
                        Err(error) => poll_health
                            .record(HealthErrorKind::Dds, format!("owned reader take: {error}")),
                    }
                }
            })
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::ResourceExhausted,
                    format!("spawn owned poller: {error}"),
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

fn decode_owned(sample: UpDdsOwnedSampleV1) -> Result<UOwnedFrame, String> {
    if !sample.has_payload && !sample.payload.is_empty() {
        return Err("owned sample has payload bytes while has_payload is false".to_owned());
    }
    let metadata = decode_frame_metadata_fields(&sample.metadata_fields)
        .map_err(|error| format!("invalid owned metadata: {error}"))?;
    let payload = sample
        .has_payload
        .then(|| bytes::Bytes::from(sample.payload));
    let frame = UOwnedFrame::new_unchecked(metadata, payload);
    frame
        .validate()
        .map_err(|error| format!("invalid owned frame: {error}"))?;
    Ok(frame)
}

#[async_trait]
impl UOwnedTransportImpl for UPTransportDdsOwned {
    async fn send_validated_owned(&self, frame: ValidatedOwnedFrame) -> Result<(), UStatus> {
        let frame = frame.into_inner();
        let metadata_fields = encode_frame_metadata_fields(frame.metadata()).map_err(|error| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("encode owned metadata: {error}"),
            )
        })?;
        let (has_payload, payload) = frame
            .payload()
            .map_or((false, Vec::new()), |payload| (true, payload.to_vec()));
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| UStatus::fail_with_code(UCode::Unavailable, "transport is closed"))?;
        let _write_guard = tracks_delivery(&self.config).then(|| self.delivery.write_guard());
        writer
            .write(
                UpDdsOwnedSampleV1 {
                    origin_id: self.config.origin_id.clone(),
                    metadata_fields,
                    has_payload,
                    payload,
                },
                None,
            )
            .map_err(|error| {
                self.health
                    .record(HealthErrorKind::Dds, format!("owned writer: {error}"));
                dds_status("write owned sample", error)
            })?;
        complete_send(
            writer,
            &self.config,
            &self.health,
            &self.delivery,
            "acknowledge owned sample",
        )
    }

    async fn register_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        self.registrations
            .register(source_filter, sink_filter, listener)
    }

    async fn unregister_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        self.registrations
            .unregister(source_filter, sink_filter, &listener)
    }
}

impl Drop for UPTransportDdsOwned {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.as_ref() {
            complete_on_drop(
                writer,
                &self.config,
                &self.health,
                &self.delivery,
                "acknowledge owned samples during teardown",
            );
        }
        self.stop.stop();
        join_worker(self.poller.take(), "owned poller", &self.health);
        self.registrations.deactivate_all();
        join_worker(self.dispatcher.take(), "owned dispatcher", &self.health);
        drop(self.writer.take());
        if let Some(participant) = self.participant.take() {
            if let Err(error) = participant.delete_contained_entities() {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete owned contained entities: {error}"),
                );
            }
            if let Err(error) =
                DomainParticipantFactory::get_instance().delete_participant(&participant)
            {
                self.health.record(
                    HealthErrorKind::Teardown,
                    format!("delete owned participant: {error}"),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_rejects_inconsistent_payload_presence() {
        let error = decode_owned(UpDdsOwnedSampleV1 {
            origin_id: "peer".to_owned(),
            metadata_fields: Vec::new(),
            has_payload: false,
            payload: vec![1],
        })
        .expect_err("inconsistent sample must fail");
        assert!(error.contains("has_payload"));
    }
}
