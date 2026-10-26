// SPDX-License-Identifier: Apache-2.0
//! Live classic and owned RTPS contracts; encoded-owned joins in its own unit.
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};
use up_rust::{
    PayloadEncoding, UCode, UFrameMetadata, UListener, UMessage, UMessageBuilder, UOwnedFrame,
    UOwnedListener, UOwnedTransport, UTransport, UUri,
};
use up_transport_dds::owned::UPTransportDdsOwned;
use up_transport_dds::{DdsConfig, UPTransportDds};
const WAIT: Duration = Duration::from_secs(8);
static DDS_TEST_LOCK: Mutex<()> = Mutex::const_new(());
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

fn topic(tag: u16) -> UUri {
    UUri::try_from_parts("dds-tests", 0x10_000 + u32::from(tag), 1, 0x8000 + tag)
        .expect("valid topic")
}
fn metadata(source: &UUri, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
    let message = UMessageBuilder::publish(source.clone())
        .build()
        .expect("message");
    match encoding {
        Some(encoding) => message
            .attributes()
            .to_frame_metadata(encoding)
            .expect("payload encoding"),
        None => message
            .attributes()
            .to_frame_metadata_unencoded()
            .expect("metadata"),
    }
}
fn trace_acknowledgement_if_requested() {
    if let Ok(filter) = std::env::var("UP_DDS_TRACE") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(false)
            .try_init();
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
            .build_with_payload(Vec::<u8>::new(), PayloadEncoding::RAW)
            .expect("empty"),
        UMessageBuilder::publish(source)
            .build_with_payload(b"nonempty".to_vec(), PayloadEncoding::RAW)
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_classic_write_survives_immediate_producer_drop() {
    let _guard = DDS_TEST_LOCK.lock().await;
    trace_acknowledgement_if_requested();
    let source = topic(21);
    let sender = UPTransportDds::new(210, tokio::runtime::Handle::current()).unwrap();
    let receiver = UPTransportDds::new(210, tokio::runtime::Handle::current()).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_listener(&source, None, Arc::new(MessageChannel(tx)))
        .await
        .unwrap();
    sender.wait_ready(2, WAIT).unwrap();
    let message = UMessageBuilder::publish(source)
        .build_with_payload(b"one reliable write".to_vec(), PayloadEncoding::RAW)
        .unwrap();
    sender.send(message).await.unwrap();
    sender.wait_acknowledged(WAIT).unwrap();
    assert_eq!(
        sender
            .wait_acknowledged(Duration::from_secs(1_u64 << 31))
            .unwrap_err()
            .code(),
        UCode::InvalidArgument
    );
    drop(sender);
    let delivered = tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered.payload().unwrap().as_ref(), b"one reliable write");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_owned_write_survives_immediate_producer_drop() {
    let _guard = DDS_TEST_LOCK.lock().await;
    trace_acknowledgement_if_requested();
    let source = topic(22);
    let sender = UPTransportDdsOwned::new(211, tokio::runtime::Handle::current()).unwrap();
    let receiver = UPTransportDdsOwned::new(211, tokio::runtime::Handle::current()).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    receiver
        .register_owned_listener(&source, None, Arc::new(OwnedChannel(tx)))
        .await
        .unwrap();
    sender.wait_ready(2, WAIT).unwrap();
    sender
        .send_owned(
            UOwnedFrame::with_payload(
                metadata(&source, Some(PayloadEncoding::RAW)),
                b"one reliable owned write".to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    sender.wait_acknowledged(WAIT).unwrap();
    assert_eq!(
        sender
            .wait_acknowledged(Duration::from_secs(1_u64 << 31))
            .unwrap_err()
            .code(),
        UCode::InvalidArgument
    );
    drop(sender);
    let delivered = tokio::time::timeout(WAIT, rx.recv())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "owned delivery after acknowledgement: {error}; receiver health: {:?}",
                receiver.health().snapshot()
            )
        })
        .unwrap();
    assert_eq!(delivered.payload_bytes(), b"one reliable owned write");
}
