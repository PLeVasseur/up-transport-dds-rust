// SPDX-License-Identifier: Apache-2.0
//! Direct middleware boundary: no uProtocol callbacks, SDK adapters or dispatcher.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::time::{Duration, Instant};

use dust_dds::{
    domain::{
        domain_participant::DomainParticipant, domain_participant_factory::DomainParticipantFactory,
    },
    infrastructure::{
        listener::NO_LISTENER,
        qos::{DataReaderQos, DataWriterQos, QosKind},
        qos_policy::{HistoryQosPolicyKind, ReliabilityQosPolicyKind},
        sample_info::{ANY_INSTANCE_STATE, ANY_SAMPLE_STATE, ANY_VIEW_STATE},
        status::NO_STATUS,
    },
    subscription::data_reader::DataReader,
    topic_definition::topic::Topic,
};
use up_transport_dds::owned::{UpDdsOwnedSampleV1, OWNED_TOPIC_V1, OWNED_TYPE_V1};

struct Participant(DomainParticipant);

impl Participant {
    fn new(domain: i32) -> Self {
        Self(
            DomainParticipantFactory::get_instance()
                .create_participant(domain, QosKind::Default, NO_LISTENER, NO_STATUS)
                .unwrap(),
        )
    }
}

impl Drop for Participant {
    fn drop(&mut self) {
        self.0.delete_contained_entities().unwrap();
        DomainParticipantFactory::get_instance()
            .delete_participant(&self.0)
            .unwrap();
    }
}

struct Poller {
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Poller {
    fn start(reader: DataReader<UpDdsOwnedSampleV1>) -> (Self, mpsc::Receiver<UpDdsOwnedSampleV1>) {
        let stop = Arc::new(AtomicBool::new(false));
        let (received, samples) = mpsc::channel();
        let poll_stop = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            while !poll_stop.load(Ordering::Acquire) {
                if let Ok(batch) =
                    reader.take(32, ANY_SAMPLE_STATE, ANY_VIEW_STATE, ANY_INSTANCE_STATE)
                {
                    for sample in batch.into_iter().filter_map(|sample| sample.data) {
                        let _ = received.send(sample);
                    }
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        });
        (
            Self {
                stop,
                worker: Some(worker),
            },
            samples,
        )
    }
}

impl Drop for Poller {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
    }
}

fn sample_topic(participant: &DomainParticipant) -> Topic {
    participant
        .create_topic::<UpDdsOwnedSampleV1>(
            OWNED_TOPIC_V1,
            OWNED_TYPE_V1,
            QosKind::Default,
            NO_LISTENER,
            NO_STATUS,
        )
        .unwrap()
}

#[test]
fn native_acknowledged_sample_survives_writer_participant_deletion() {
    if let Ok(filter) = std::env::var("UP_DDS_TRACE") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .try_init();
    }
    for iteration in 0..20 {
        let producer = Participant::new(80 + iteration);
        let consumer = Participant::new(80 + iteration);
        let topic = sample_topic(&producer.0);
        let remote_topic = sample_topic(&consumer.0);
        let mut writer_qos = DataWriterQos::default();
        writer_qos.reliability.kind = ReliabilityQosPolicyKind::Reliable;
        writer_qos.history.kind = HistoryQosPolicyKind::KeepLast(32);
        let mut reader_qos = DataReaderQos::default();
        reader_qos.reliability.kind = ReliabilityQosPolicyKind::Reliable;
        reader_qos.history.kind = HistoryQosPolicyKind::KeepLast(32);
        let publisher = producer
            .0
            .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
            .unwrap();
        let writer = publisher
            .create_datawriter::<UpDdsOwnedSampleV1>(
                &topic,
                QosKind::Specific(writer_qos),
                NO_LISTENER,
                NO_STATUS,
            )
            .unwrap();
        let local_subscriber = producer
            .0
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .unwrap();
        let _local_reader = local_subscriber
            .create_datareader::<UpDdsOwnedSampleV1>(
                &topic,
                QosKind::Specific(reader_qos.clone()),
                NO_LISTENER,
                NO_STATUS,
            )
            .unwrap();
        let subscriber = consumer
            .0
            .create_subscriber(QosKind::Default, NO_LISTENER, NO_STATUS)
            .unwrap();
        let reader = subscriber
            .create_datareader::<UpDdsOwnedSampleV1>(
                &remote_topic,
                QosKind::Specific(reader_qos),
                NO_LISTENER,
                NO_STATUS,
            )
            .unwrap();
        let (_poller, samples) = Poller::start(reader.clone());
        let deadline = Instant::now() + Duration::from_secs(8);
        while !writer
            .get_matched_subscriptions()
            .unwrap()
            .contains(&reader.get_instance_handle())
        {
            assert!(
                Instant::now() < deadline,
                "native discovery at iteration {iteration}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        writer
            .write(
                UpDdsOwnedSampleV1 {
                    origin_id: format!("native-only-{iteration}"),
                    metadata_fields: vec![],
                    has_payload: true,
                    payload: b"one native write".to_vec(),
                },
                None,
            )
            .unwrap();
        writer
            .wait_for_acknowledgments(dust_dds::infrastructure::time::Duration::new(8, 0))
            .unwrap_or_else(|error| panic!("native acknowledgement at iteration {iteration}: {error:?}; writer matches: {:?}; reader matches: {:?}", writer.get_publication_matched_status(), reader.get_subscription_matched_status()));
        drop(producer);
        let sample = samples.recv_timeout(Duration::from_secs(8)).unwrap_or_else(|error| {
            panic!("native DDS lost acknowledged sample at iteration {iteration}: {error}; matches: {:?}", reader.get_subscription_matched_status())
        });
        assert_eq!(sample.payload, b"one native write");
    }
}
