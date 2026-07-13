// SPDX-License-Identifier: Apache-2.0
//! Live RTPS contract tests for all DDS carriage families.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dust_dds::domain::domain_participant::DomainParticipant;
use dust_dds::domain::domain_participant_factory::DomainParticipantFactory;
use dust_dds::infrastructure::qos::QosKind;
use dust_dds::infrastructure::status::NO_STATUS;
use dust_dds::listener::NO_LISTENER;
use dust_dds::publication::data_writer::DataWriter;
use tokio::sync::{mpsc, Mutex, Notify};
use up_rust::selected_wire_user_api::UWithNativePrefixWire as _;
use up_rust::transport_implementer_api::{
    UEncodedRxFrame, UEncodedZeroCopyListener, UZeroCopyTransportCore,
};
use up_rust::{
    try_project_umessage_to_frame_metadata, EncodePayload, PayloadEncoding, PayloadFormat, UCode,
    UFrameMetadata, UFrameView, UListener, UMessage, UMessageBuilder, UOwnedFrame, UOwnedListener,
    UOwnedTransport, UPayloadFormat, UTransport, UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri,
    UZeroCopyListener, UZeroCopyRxLease, UZeroCopyTransport,
};
use up_transport_dds::owned::{
    UPTransportDdsOwned, UpDdsOwnedSampleV1, OWNED_TOPIC_V1, OWNED_TYPE_V1,
};
use up_transport_dds::zero_copy::{DdsRxFrame, DdsZeroCopyCore};
use up_transport_dds::{AcknowledgmentMode, DdsConfig, DdsHealth, UPTransportDds};

const WAIT: Duration = Duration::from_secs(8);
static DDS_TEST_LOCK: Mutex<()> = Mutex::const_new(());

fn topic(tag: u16) -> UUri {
    UUri::try_from_parts("dds-tests", 0x10_000 + u32::from(tag), 1, 0x8000 + tag)
        .expect("valid topic")
}

fn endpoint(authority: &str) -> UUri {
    UUri::try_from_parts(authority, 0x1_2345, 1, 0).expect("valid endpoint")
}

fn metadata(source: &UUri, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
    let message = UMessageBuilder::publish(source.clone())
        .build()
        .expect("message");
    let metadata = try_project_umessage_to_frame_metadata(&message).expect("metadata");
    match encoding {
        Some(encoding) => metadata
            .with_payload_encoding(encoding)
            .expect("payload encoding"),
        None => metadata,
    }
}

struct MessageChannel(mpsc::UnboundedSender<UMessage>);

#[async_trait]
impl UListener for MessageChannel {
    async fn on_receive(&self, message: UMessage) {
        let _ = self.0.send(message);
    }
}

struct OwnedChannel(mpsc::UnboundedSender<UOwnedFrame>);

#[async_trait]
impl UOwnedListener for OwnedChannel {
    async fn on_receive_owned(&self, frame: UOwnedFrame) {
        let _ = self.0.send(frame);
    }
}

struct FrameChannel<Rx>(mpsc::UnboundedSender<Rx>);

#[async_trait]
impl<Rx> UZeroCopyListener<Rx> for FrameChannel<Rx>
where
    Rx: UZeroCopyRxLease + Send + 'static,
{
    async fn on_receive_zero_copy(&self, frame: Rx) {
        let _ = self.0.send(frame);
    }
}

struct EncodedChannel(mpsc::UnboundedSender<DdsRxFrame>);

#[async_trait]
impl UEncodedZeroCopyListener<DdsRxFrame> for EncodedChannel {
    async fn on_receive_encoded_zero_copy(&self, frame: DdsRxFrame) {
        let _ = self.0.send(frame);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classic_distinct_instances_preserve_payload_presence_and_suppress_self() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(1);
    let sender = UPTransportDds::with_config(
        DdsConfig {
            origin_id: "classic-sender".to_owned(),
            ..DdsConfig::new(171)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("sender");
    let receiver = UPTransportDds::with_config(
        DdsConfig {
            origin_id: "classic-receiver".to_owned(),
            ..DdsConfig::new(171)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("receiver");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    receiver.wait_ready(2, WAIT).expect("receiver discovery");

    let (remote_tx, mut remote_rx) = mpsc::unbounded_channel();
    receiver
        .register_listener(&source, None, Arc::new(MessageChannel(remote_tx)))
        .await
        .expect("remote listener");
    let (self_tx, mut self_rx) = mpsc::unbounded_channel();
    sender
        .register_listener(&source, None, Arc::new(MessageChannel(self_tx)))
        .await
        .expect("self listener");

    let messages = [
        UMessageBuilder::publish(source.clone())
            .build()
            .expect("absent"),
        UMessageBuilder::publish(source.clone())
            .build_with_payload(Vec::<u8>::new(), UPayloadFormat::Raw)
            .expect("empty"),
        UMessageBuilder::publish(source)
            .build_with_payload(b"nonempty".to_vec(), UPayloadFormat::Raw)
            .expect("nonempty"),
    ];
    for message in messages {
        sender.send(message).await.expect("send");
    }

    let absent = tokio::time::timeout(WAIT, remote_rx.recv())
        .await
        .expect("receive timeout")
        .expect("absent message");
    let empty = tokio::time::timeout(WAIT, remote_rx.recv())
        .await
        .expect("receive timeout")
        .expect("empty message");
    let nonempty = tokio::time::timeout(WAIT, remote_rx.recv())
        .await
        .expect("receive timeout")
        .expect("nonempty message");
    assert!(absent.payload().is_none());
    assert_eq!(empty.payload().expect("present empty").len(), 0);
    assert_eq!(nonempty.payload().expect("present").as_ref(), b"nonempty");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), self_rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_families_validate_filters_and_none_requires_no_sink() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let transport = UPTransportDds::new(172, tokio::runtime::Handle::current()).expect("transport");
    let invalid = endpoint("invalid-source").clone_with_resource_id(1);
    let (tx, _rx) = mpsc::unbounded_channel();
    let error = transport
        .register_listener(&invalid, None, Arc::new(MessageChannel(tx)))
        .await
        .expect_err("invalid filter must fail");
    assert_eq!(error.get_code(), UCode::InvalidArgument);

    let owned = UPTransportDdsOwned::new(172, tokio::runtime::Handle::current()).expect("owned");
    let (tx, _rx) = mpsc::unbounded_channel();
    let error = owned
        .register_owned_listener(&invalid, None, Arc::new(OwnedChannel(tx)))
        .await
        .expect_err("invalid owned filter must fail");
    assert_eq!(error.get_code(), UCode::InvalidArgument);

    let zero_copy = DdsZeroCopyCore::new(172, tokio::runtime::Handle::current())
        .expect("zero-copy")
        .into_native_prefix_wire_transport(up_rust::ProtobufWire);
    let (tx, _rx) = mpsc::unbounded_channel();
    let error = zero_copy
        .register_zero_copy_listener(&invalid, None, Arc::new(FrameChannel(tx)))
        .await
        .expect_err("invalid zero-copy filter must fail");
    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn none_sink_filter_matches_only_sinkless_messages() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(12);
    let sink = endpoint("sink-filter");
    let sender = UPTransportDds::new(183, tokio::runtime::Handle::current()).expect("sender");
    let receiver = UPTransportDds::new(183, tokio::runtime::Handle::current()).expect("receiver");
    sender.wait_ready(2, WAIT).expect("sender discovery");

    let (sinkless_tx, mut sinkless_rx) = mpsc::unbounded_channel();
    receiver
        .register_listener(&source, None, Arc::new(MessageChannel(sinkless_tx)))
        .await
        .expect("sinkless listener");
    let (addressed_tx, mut addressed_rx) = mpsc::unbounded_channel();
    receiver
        .register_listener(&source, Some(&sink), Arc::new(MessageChannel(addressed_tx)))
        .await
        .expect("addressed listener");

    sender
        .send(
            UMessageBuilder::notification(source.clone(), sink)
                .build()
                .expect("notification"),
        )
        .await
        .expect("send notification");
    tokio::time::timeout(WAIT, addressed_rx.recv())
        .await
        .expect("addressed timeout")
        .expect("addressed message");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), sinkless_rx.recv())
            .await
            .is_err()
    );

    sender
        .send(UMessageBuilder::publish(source).build().expect("publish"))
        .await
        .expect("send publish");
    tokio::time::timeout(WAIT, sinkless_rx.recv())
        .await
        .expect("sinkless timeout")
        .expect("sinkless message");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), addressed_rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owned_distinct_instances_preserve_absent_empty_and_nonempty() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(3);
    let sender = UPTransportDdsOwned::new(173, tokio::runtime::Handle::current()).expect("sender");
    let receiver =
        UPTransportDdsOwned::new(173, tokio::runtime::Handle::current()).expect("receiver");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    receiver.wait_ready(2, WAIT).expect("receiver discovery");
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_owned_listener(&source, None, Arc::new(OwnedChannel(tx)))
        .await
        .expect("register");
    let (self_tx, mut self_rx) = mpsc::unbounded_channel();
    sender
        .register_owned_listener(&source, None, Arc::new(OwnedChannel(self_tx)))
        .await
        .expect("self register");

    let frames = [
        UOwnedFrame::without_payload(metadata(&source, None)).expect("absent"),
        UOwnedFrame::with_payload(
            metadata(&source, Some(PayloadEncoding::RAW)),
            Vec::<u8>::new(),
        )
        .expect("empty"),
        UOwnedFrame::with_payload(
            metadata(&source, Some(PayloadEncoding::RAW)),
            b"owned".to_vec(),
        )
        .expect("nonempty"),
    ];
    for frame in frames {
        sender.send_owned(frame).await.expect("send");
    }

    let absent = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("absent");
    let empty = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("empty");
    let nonempty = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("nonempty");
    assert!(!absent.has_payload());
    assert!(empty.has_payload());
    assert!(empty.payload_bytes().is_empty());
    assert_eq!(nonempty.payload_bytes(), b"owned");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), self_rx.recv())
            .await
            .is_err()
    );
}

macro_rules! selected_wire_round_trip {
    ($name:ident, $domain:expr, $tag:expr, $wire:expr, $wire_ty:ty, $value:expr, $value_ty:ty) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $name() {
            let _test_guard = DDS_TEST_LOCK.lock().await;
            let source = topic($tag);
            let sender_core = DdsZeroCopyCore::with_config(
                DdsConfig {
                    acknowledgment_mode: AcknowledgmentMode::PerSend,
                    ..DdsConfig::new($domain)
                },
                tokio::runtime::Handle::current(),
            )
            .expect("sender");
            let receiver_core =
                DdsZeroCopyCore::new($domain, tokio::runtime::Handle::current()).expect("receiver");
            sender_core.wait_ready(2, WAIT).expect("sender discovery");
            receiver_core
                .wait_ready(2, WAIT)
                .expect("receiver discovery");
            let sender = sender_core.into_native_prefix_wire_transport($wire);
            let receiver = receiver_core.into_native_prefix_wire_transport($wire);
            let (tx, mut rx) = mpsc::unbounded_channel();
            receiver
                .register_zero_copy_listener(&source, None, Arc::new(FrameChannel(tx)))
                .await
                .expect("register");

            let value: $value_ty = $value;
            let layout = <$wire_ty as EncodePayload<$value_ty>>::payload_layout(&value)
                .expect("payload layout");
            let mut loan = sender
                .loan_tx(
                    UTxLoanSpec::payload(
                        metadata(&source, Some(<$wire_ty as PayloadFormat>::encoding())),
                        layout.len(),
                        layout.align(),
                    )
                    .expect("loan spec"),
                )
                .await
                .expect("loan");
            <$wire_ty as EncodePayload<$value_ty>>::encode_payload(&value, loan.payload_mut())
                .expect("encode");
            sender.send_zero_copy(loan).await.expect("send");
            let frame = tokio::time::timeout(WAIT, rx.recv())
                .await
                .expect("timeout")
                .expect("frame");
            let decoded: $value_ty = frame.decode_payload().expect("decode");
            assert_eq!(decoded, value);
        }
    };
}

selected_wire_round_trip!(
    xcdrv2_round_trip,
    174,
    4,
    up_wire_xcdrv2::XcdrV2Wire,
    up_wire_xcdrv2::XcdrV2Wire,
    up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_VALUE,
    up_wire_xcdrv2::VehicleSignalV1
);

selected_wire_round_trip!(
    arrow_round_trip,
    175,
    5,
    up_wire_arrow::ArrowWire,
    up_wire_arrow::ArrowWire,
    up_wire_arrow::TelemetryTableV1::fixture(64, 7),
    up_wire_arrow::TelemetryTableV1
);

selected_wire_round_trip!(
    omg_idl_round_trip,
    176,
    6,
    up_wire_omgidl::OmgIdlWire,
    up_wire_omgidl::OmgIdlWire,
    up_wire_omgidl::VehicleStatusV1::fixture(42),
    up_wire_omgidl::VehicleStatusV1
);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialized_and_uninitialized_loans_honor_requested_alignment() {
    use up_rust::transport_implementer_api::{PreparedTxLoanSpec, UZeroCopyTransportCore};
    use up_rust::UZeroCopyUninitTransportCore;

    let _test_guard = DDS_TEST_LOCK.lock().await;

    let core = DdsZeroCopyCore::new(177, tokio::runtime::Handle::current()).expect("core");
    let source = topic(7);
    for alignment in [1, 2, 8, 64, 4096] {
        let prepared = PreparedTxLoanSpec::from_encoded_parts(
            metadata(&source, Some(PayloadEncoding::RAW)),
            vec![1, 2, 3],
            33,
            alignment,
        )
        .expect("prepared");
        let initialized = core
            .loan_prepared_tx(prepared.clone())
            .await
            .expect("initialized loan");
        assert_eq!(initialized.payload().as_ptr() as usize % alignment, 0);

        let mut uninitialized = core
            .loan_prepared_uninit_tx(prepared)
            .await
            .expect("uninitialized loan");
        assert_eq!(
            uninitialized.payload_uninit_mut().as_ptr() as usize % alignment,
            0
        );
        for byte in uninitialized.payload_uninit_mut() {
            byte.write(0xA5);
        }
        // SAFETY: every byte in the visible payload range was initialized above.
        let initialized = unsafe { uninitialized.assume_payload_init() };
        assert!(initialized.payload().iter().all(|byte| *byte == 0xA5));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_wire_mismatch_is_rejected_before_user_callback() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(9);
    let sender_core = DdsZeroCopyCore::new(180, tokio::runtime::Handle::current()).expect("sender");
    let receiver_core =
        DdsZeroCopyCore::new(180, tokio::runtime::Handle::current()).expect("receiver");
    sender_core.wait_ready(2, WAIT).expect("sender discovery");
    let sender = sender_core.into_native_prefix_wire_transport(up_wire_xcdrv2::XcdrV2Wire);
    let receiver = receiver_core.into_native_prefix_wire_transport(up_wire_arrow::ArrowWire);
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_zero_copy_listener(&source, None, Arc::new(FrameChannel(tx)))
        .await
        .expect("register");

    let value = up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_VALUE;
    let layout = <up_wire_xcdrv2::XcdrV2Wire as EncodePayload<
        up_wire_xcdrv2::VehicleSignalV1,
    >>::payload_layout(&value)
    .expect("layout");
    let mut loan = sender
        .loan_tx(
            UTxLoanSpec::payload(
                metadata(&source, Some(up_wire_xcdrv2::XcdrV2Wire::encoding())),
                layout.len(),
                layout.align(),
            )
            .expect("spec"),
        )
        .await
        .expect("loan");
    <up_wire_xcdrv2::XcdrV2Wire as EncodePayload<up_wire_xcdrv2::VehicleSignalV1>>::encode_payload(
        &value,
        loan.payload_mut(),
    )
    .expect("encode");
    sender.send_zero_copy(loan).await.expect("send");
    assert!(tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_copy_preserves_absent_empty_and_nonempty_payloads() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(11);
    let sender_core = DdsZeroCopyCore::new(182, tokio::runtime::Handle::current()).expect("sender");
    let receiver_core =
        DdsZeroCopyCore::new(182, tokio::runtime::Handle::current()).expect("receiver");
    sender_core.wait_ready(2, WAIT).expect("discovery");
    let sender = sender_core.into_native_prefix_wire_transport(up_rust::ProtobufWire);
    let receiver = receiver_core.into_native_prefix_wire_transport(up_rust::ProtobufWire);
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_zero_copy_listener(&source, None, Arc::new(FrameChannel(tx)))
        .await
        .expect("register");
    let (self_tx, mut self_rx) = mpsc::unbounded_channel();
    sender
        .register_zero_copy_listener(&source, None, Arc::new(FrameChannel(self_tx)))
        .await
        .expect("self register");

    let absent = sender
        .loan_tx(UTxLoanSpec::no_payload(metadata(&source, None)).expect("absent spec"))
        .await
        .expect("absent loan");
    sender.send_zero_copy(absent).await.expect("send absent");
    let empty = sender
        .loan_tx(
            UTxLoanSpec::present_empty_payload(metadata(
                &source,
                Some(up_rust::ProtobufWire::encoding()),
            ))
            .expect("empty spec"),
        )
        .await
        .expect("empty loan");
    sender.send_zero_copy(empty).await.expect("send empty");
    let mut nonempty = sender
        .loan_tx(
            UTxLoanSpec::payload(
                metadata(&source, Some(up_rust::ProtobufWire::encoding())),
                3,
                8,
            )
            .expect("nonempty spec"),
        )
        .await
        .expect("nonempty loan");
    nonempty.payload_mut().copy_from_slice(b"abc");
    sender
        .send_zero_copy(nonempty)
        .await
        .expect("send nonempty");

    let absent = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("absent frame");
    let empty = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("empty frame");
    let nonempty = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("timeout")
        .expect("nonempty frame");
    assert!(absent.metadata().payload_encoding().is_none());
    assert_eq!(absent.payload_len(), 0);
    assert!(empty.metadata().payload_encoding().is_some());
    assert_eq!(empty.payload_len(), 0);
    assert_eq!(nonempty.try_contiguous_payload(), Some(b"abc".as_slice()));
    assert!(
        tokio::time::timeout(Duration::from_millis(250), self_rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_copy_core_carries_metadata_opaquely_and_routes_from_sideband() {
    use up_rust::transport_implementer_api::{PreparedTxLoanSpec, UZeroCopyTransportCore};

    let _test_guard = DDS_TEST_LOCK.lock().await;

    let source = topic(13);
    let sender = DdsZeroCopyCore::with_config(
        DdsConfig {
            acknowledgment_mode: AcknowledgmentMode::PerSend,
            ..DdsConfig::new(186)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("sender");
    let receiver = DdsZeroCopyCore::new(186, tokio::runtime::Handle::current()).expect("receiver");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_encoded_zero_copy_listener(&source, None, Arc::new(EncodedChannel(tx)))
        .await
        .expect("register encoded listener");

    let encoded_metadata = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let prepared = PreparedTxLoanSpec::from_encoded_parts(
        metadata(&source, Some(PayloadEncoding::RAW)),
        encoded_metadata.clone(),
        4,
        64,
    )
    .expect("prepared loan");
    let mut loan = sender.loan_prepared_tx(prepared).await.expect("loan");
    loan.payload_mut().copy_from_slice(b"data");
    sender
        .send_prepared_zero_copy(loan)
        .await
        .expect("send encoded frame");

    let frame = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("receive timeout")
        .expect("encoded frame");
    assert_eq!(frame.encoded_metadata(), encoded_metadata);
    assert_eq!(frame.try_contiguous_payload(), Some(b"data".as_slice()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_owned_sample_from_distinct_participant_is_observable() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let receiver =
        UPTransportDdsOwned::new(187, tokio::runtime::Handle::current()).expect("owned receiver");
    let (participant, writer) = raw_owned_writer(187);
    wait_for_writer_match(&writer);
    writer
        .write(
            UpDdsOwnedSampleV1 {
                origin_id: "malformed-peer".to_owned(),
                metadata_fields: vec![0xFF],
                has_payload: false,
                payload: Vec::new(),
            },
            None,
        )
        .expect("write malformed sample");

    tokio::time::timeout(WAIT, async {
        while receiver.health().snapshot().malformed_samples == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("malformed sample was not observed");
    assert_eq!(receiver.health().snapshot().malformed_samples, 1);

    drop(writer);
    participant
        .delete_contained_entities()
        .expect("delete raw writer entities");
    DomainParticipantFactory::get_instance()
        .delete_participant(&participant)
        .expect("delete raw writer participant");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_dispatch_reports_backpressure() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(10);
    let sender = UPTransportDds::new(181, tokio::runtime::Handle::current()).expect("sender");
    let receiver = UPTransportDds::with_config(
        DdsConfig {
            dispatch_capacity: 1,
            ..DdsConfig::new(181)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("receiver");
    sender.wait_ready(2, WAIT).expect("discovery");
    let listener = Arc::new(BlockingListener {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Notify::new(),
    });
    receiver
        .register_listener(&source, None, listener.clone())
        .await
        .expect("register");
    for _ in 0..16 {
        sender
            .send(
                UMessageBuilder::publish(source.clone())
                    .build()
                    .expect("message"),
            )
            .await
            .expect("send");
    }
    tokio::time::timeout(WAIT, listener.entered.notified())
        .await
        .expect("callback entered");
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(receiver.health().snapshot().dispatch_drops > 0);
    listener.release.notify_one();
}

struct BlockingListener {
    calls: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl UListener for BlockingListener {
    async fn on_receive(&self, _message: UMessage) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.notified().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unregister_waits_for_inflight_and_prevents_later_callbacks() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(8);
    let sender = UPTransportDds::new(178, tokio::runtime::Handle::current()).expect("sender");
    let receiver = UPTransportDds::new(178, tokio::runtime::Handle::current()).expect("receiver");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    let listener = Arc::new(BlockingListener {
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: Notify::new(),
    });
    let erased: Arc<dyn UListener> = listener.clone();
    receiver
        .register_listener(&source, None, erased.clone())
        .await
        .expect("register");
    sender
        .send(
            UMessageBuilder::publish(source.clone())
                .build()
                .expect("message"),
        )
        .await
        .expect("send");
    tokio::time::timeout(WAIT, listener.entered.notified())
        .await
        .expect("callback entered");

    let release_listener = Arc::clone(&listener);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        release_listener.release.notify_one();
    });
    receiver
        .unregister_listener(&source, None, erased)
        .await
        .expect("unregister");
    sender
        .send(UMessageBuilder::publish(source).build().expect("message"))
        .await
        .expect("send after unregister");
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(listener.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_drop_rpc_responses_complete_across_all_families_and_selected_wires() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let method = endpoint("one-shot-server").clone_with_resource_id(0x1001);
    let client = endpoint("one-shot-client");
    let owned_receiver =
        UPTransportDdsOwned::new(70, tokio::runtime::Handle::current()).expect("owned receiver");
    let owned_receiver_health = owned_receiver.health();
    let (owned_tx, mut owned_rx) = mpsc::unbounded_channel();
    owned_receiver
        .register_owned_listener(&method, Some(&client), Arc::new(OwnedChannel(owned_tx)))
        .await
        .expect("owned listener");

    let payload = b"owned-arrow-one-shot".to_vec();
    let response = rpc_response(
        &method,
        &client,
        payload.clone(),
        up_wire_arrow::ArrowWire::encoding(),
    );
    let sender =
        UPTransportDdsOwned::new(70, tokio::runtime::Handle::current()).expect("owned sender");
    sender.wait_ready(2, WAIT).expect("owned discovery");
    owned_receiver
        .wait_ready(2, WAIT)
        .expect("reverse owned discovery");
    let health = sender.health();
    let metadata = try_project_umessage_to_frame_metadata(&response).expect("owned metadata");
    sender
        .send_owned(UOwnedFrame::with_payload(metadata, payload.clone()).expect("owned frame"))
        .await
        .expect("owned response");
    drop(sender);
    let owned_frame = tokio::time::timeout(WAIT, owned_rx.recv())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "owned Arrow response timed out: {error}; receiver health: {:?}",
                owned_receiver_health.snapshot()
            )
        })
        .expect("owned response");
    assert_eq!(owned_frame.payload_bytes(), payload);
    assert_bounded_completion(&health);

    classic_one_shot(192, &method, &client, b"classic-one-shot").await;

    zero_copy_one_shot(
        193,
        rpc_response(&method, &client, b"xcdr".to_vec(), PayloadEncoding::RAW),
        &method,
        &client,
        b"xcdr",
        up_wire_xcdrv2::XcdrV2Wire,
        up_wire_xcdrv2::XcdrV2Wire::encoding(),
    )
    .await;
    zero_copy_one_shot(
        194,
        rpc_response(
            &method,
            &client,
            b"arrow".to_vec(),
            up_wire_arrow::ArrowWire::encoding(),
        ),
        &method,
        &client,
        b"arrow",
        up_wire_arrow::ArrowWire,
        up_wire_arrow::ArrowWire::encoding(),
    )
    .await;
    zero_copy_one_shot(
        195,
        rpc_response(
            &method,
            &client,
            b"omgidl".to_vec(),
            up_wire_omgidl::OmgIdlWire::encoding(),
        ),
        &method,
        &client,
        b"omgidl",
        up_wire_omgidl::OmgIdlWire,
        up_wire_omgidl::OmgIdlWire::encoding(),
    )
    .await;
}

async fn classic_one_shot(domain_id: i32, method: &UUri, client: &UUri, payload: &[u8]) {
    let response = rpc_response(method, client, payload.to_vec(), PayloadEncoding::RAW);
    let receiver = UPTransportDds::new(domain_id, tokio::runtime::Handle::current())
        .expect("classic receiver");
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_listener(method, Some(client), Arc::new(MessageChannel(tx)))
        .await
        .expect("classic listener");
    let sender =
        UPTransportDds::new(domain_id, tokio::runtime::Handle::current()).expect("classic sender");
    sender.wait_ready(2, WAIT).expect("classic discovery");
    let health = sender.health();
    sender.send(response).await.expect("classic response");
    drop(sender);
    assert_clean_completion(&health);
    let message = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("classic response timeout")
        .expect("classic response");
    assert_eq!(message.payload().expect("classic payload"), payload);
}

async fn zero_copy_one_shot<W>(
    domain_id: i32,
    response: UMessage,
    method: &UUri,
    client: &UUri,
    payload: &[u8],
    wire: W,
    payload_encoding: PayloadEncoding,
) where
    W: up_rust::wire_implementer_api::UWire + Copy + Send + Sync + 'static,
    up_rust::wire_implementer_api::NativePrefixFrameMetadataCodec:
        up_rust::wire_implementer_api::UWireMetadataCodecFor<W>,
{
    let receiver =
        DdsZeroCopyCore::new(domain_id, tokio::runtime::Handle::current()).expect("receiver");
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_encoded_zero_copy_listener(method, Some(client), Arc::new(EncodedChannel(tx)))
        .await
        .expect("zero-copy listener");
    let health = send_zero_copy_response(
        domain_id,
        response,
        payload.to_vec(),
        wire,
        payload_encoding,
    )
    .await;
    assert_zero_copy_response(&mut rx, payload).await;
    assert_clean_completion(&health);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_send_acknowledgment_timeout_is_returned_and_observable() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(14);
    let receiver = UPTransportDds::new(191, tokio::runtime::Handle::current()).expect("receiver");
    let sender = UPTransportDds::with_config(
        DdsConfig {
            acknowledgment_mode: AcknowledgmentMode::PerSend,
            acknowledgment_timeout: Duration::from_millis(10),
            poll_interval: Duration::from_secs(30),
            ..DdsConfig::new(191)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("sender");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    let health = sender.health();

    let error = sender
        .send(UMessageBuilder::publish(source).build().expect("message"))
        .await
        .expect_err("paused local delivery must time out");
    assert_eq!(error.get_code(), UCode::DeadlineExceeded);
    assert!(error
        .get_message()
        .is_some_and(|message| message.contains("acknowledge classic sample")));
    drop(sender);
    let snapshot = health.snapshot();
    assert_eq!(snapshot.delivery_completion_errors, 1, "{snapshot:?}");
    assert_eq!(
        snapshot.recent_events.last().expect("health event").kind,
        up_transport_dds::HealthErrorKind::DeliveryCompletion
    );
    drop(receiver);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_drop_acknowledgment_timeout_is_observable() {
    let _test_guard = DDS_TEST_LOCK.lock().await;
    let source = topic(15);
    let receiver = UPTransportDds::new(196, tokio::runtime::Handle::current()).expect("receiver");
    let sender = UPTransportDds::with_config(
        DdsConfig {
            acknowledgment_timeout: Duration::from_millis(10),
            poll_interval: Duration::from_secs(30),
            ..DdsConfig::new(196)
        },
        tokio::runtime::Handle::current(),
    )
    .expect("sender");
    sender.wait_ready(2, WAIT).expect("sender discovery");
    let health = sender.health();

    sender
        .send(UMessageBuilder::publish(source).build().expect("message"))
        .await
        .expect("on-drop mode defers completion");
    drop(sender);

    let snapshot = health.snapshot();
    assert_eq!(snapshot.delivery_completion_errors, 1, "{snapshot:?}");
    assert_eq!(
        snapshot.recent_events.last().expect("health event").kind,
        up_transport_dds::HealthErrorKind::DeliveryCompletion
    );
    drop(receiver);
}

fn rpc_response(
    method: &UUri,
    client: &UUri,
    payload: Vec<u8>,
    encoding: PayloadEncoding,
) -> UMessage {
    let request = UMessageBuilder::request(method.clone(), client.clone(), 1_000)
        .build()
        .expect("request");
    UMessageBuilder::response(
        request.source().clone(),
        request.id().clone(),
        request.sink().expect("request method").clone(),
    )
    .build_with_payload_encoding(payload, encoding)
    .expect("response")
}

async fn send_zero_copy_response<W>(
    domain_id: i32,
    response: UMessage,
    payload: Vec<u8>,
    wire: W,
    payload_encoding: PayloadEncoding,
) -> DdsHealth
where
    W: up_rust::wire_implementer_api::UWire + Copy + Send + Sync + 'static,
    up_rust::wire_implementer_api::NativePrefixFrameMetadataCodec:
        up_rust::wire_implementer_api::UWireMetadataCodecFor<W>,
{
    let core = DdsZeroCopyCore::new(domain_id, tokio::runtime::Handle::current()).expect("sender");
    core.wait_ready(2, WAIT).expect("zero-copy discovery");
    let health = core.health();
    let transport = core.into_native_prefix_wire_transport(wire);
    let metadata = try_project_umessage_to_frame_metadata(&response)
        .expect("zero-copy metadata")
        .with_payload_encoding(payload_encoding)
        .expect("selected-wire encoding");
    let mut loan = transport
        .loan_tx(UTxLoanSpec::payload(metadata, payload.len(), 8).expect("loan spec"))
        .await
        .expect("loan");
    loan.payload_mut().copy_from_slice(&payload);
    transport
        .send_zero_copy(loan)
        .await
        .expect("zero-copy response");
    drop(transport);
    health
}

async fn assert_zero_copy_response(
    rx: &mut mpsc::UnboundedReceiver<DdsRxFrame>,
    expected_payload: &[u8],
) {
    let frame = tokio::time::timeout(WAIT, rx.recv())
        .await
        .expect("zero-copy response timeout")
        .expect("zero-copy response");
    assert_eq!(frame.try_contiguous_payload(), Some(expected_payload));
}

fn assert_clean_completion(health: &DdsHealth) {
    let snapshot = health.snapshot();
    assert_eq!(snapshot.delivery_completion_errors, 0, "{snapshot:?}");
    assert_eq!(snapshot.teardown_errors, 0, "{snapshot:?}");
}

fn assert_bounded_completion(health: &DdsHealth) {
    let snapshot = health.snapshot();
    assert!(snapshot.delivery_completion_errors <= 1, "{snapshot:?}");
    assert_eq!(snapshot.teardown_errors, 0, "{snapshot:?}");
    if let Some(event) = snapshot.recent_events.last() {
        assert_eq!(
            event.kind,
            up_transport_dds::HealthErrorKind::DeliveryCompletion
        );
        assert!(event.detail.contains("acknowledge"), "{event:?}");
    }
}

#[test]
fn shutdown_is_joined_and_domain_can_restart() {
    let _test_guard = DDS_TEST_LOCK.blocking_lock();
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for _ in 0..2 {
        let transport = UPTransportDds::new(179, runtime.handle().clone()).expect("transport");
        transport.wait_ready(1, WAIT).expect("local discovery");
        let health = transport.health();
        drop(transport);
        assert_eq!(health.snapshot().worker_panics, 0);
        assert_eq!(health.snapshot().teardown_errors, 0);
    }
    for _ in 0..2 {
        let transport =
            UPTransportDdsOwned::new(184, runtime.handle().clone()).expect("owned transport");
        transport.wait_ready(1, WAIT).expect("owned discovery");
        let health = transport.health();
        drop(transport);
        assert_eq!(health.snapshot().worker_panics, 0);
        assert_eq!(health.snapshot().teardown_errors, 0);
    }
    for _ in 0..2 {
        let transport =
            DdsZeroCopyCore::new(185, runtime.handle().clone()).expect("zero-copy transport");
        transport.wait_ready(1, WAIT).expect("zero-copy discovery");
        let health = transport.health();
        drop(transport);
        assert_eq!(health.snapshot().worker_panics, 0);
        assert_eq!(health.snapshot().teardown_errors, 0);
    }
}

fn raw_owned_writer(domain_id: i32) -> (DomainParticipant, DataWriter<UpDdsOwnedSampleV1>) {
    let factory = DomainParticipantFactory::get_instance();
    let participant = factory
        .create_participant(domain_id, QosKind::Default, NO_LISTENER, NO_STATUS)
        .expect("create raw participant");
    let topic = participant
        .create_topic::<UpDdsOwnedSampleV1>(
            OWNED_TOPIC_V1,
            OWNED_TYPE_V1,
            QosKind::Default,
            NO_LISTENER,
            NO_STATUS,
        )
        .expect("create raw topic");
    let publisher = participant
        .create_publisher(QosKind::Default, NO_LISTENER, NO_STATUS)
        .expect("create raw publisher");
    let writer = publisher
        .create_datawriter::<UpDdsOwnedSampleV1>(&topic, QosKind::Default, NO_LISTENER, NO_STATUS)
        .expect("create raw writer");
    (participant, writer)
}

fn wait_for_writer_match(writer: &DataWriter<UpDdsOwnedSampleV1>) {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        if writer
            .get_publication_matched_status()
            .expect("raw writer matched status")
            .current_count
            >= 1
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "raw writer discovery timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
