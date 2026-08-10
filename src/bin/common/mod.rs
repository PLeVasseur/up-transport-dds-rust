// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use tokio::sync::mpsc;
use up_rust::{
    verify_filter_criteria, NativePrefixFrameMetadataCodec, PayloadCodecIdentity, PayloadEncoding,
    UCode, UFrameMetadata, UFrameView, UListener, UMessage, UMessageBuilder, UOwnedFrame,
    UOwnedListener, UOwnedTransport, UStatus, UTransport, UTxBuffer, UTxLoanSpec, UUri, UWire,
    UWireMetadataCodecFor, UWithNativePrefixWire as _, UZeroCopyListener, UZeroCopyRxLease,
    UZeroCopyTransportImpl,
};
use up_transport_dds::owned::UPTransportDdsOwned;
use up_transport_dds::zero_copy::DdsZeroCopyCore;
use up_transport_dds::{DdsConfig, Reliability, UPTransportDds};

const ENTITY_ID: u32 = 0x5BA0;
const ENTITY_VERSION: u8 = 1;

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) enum Role {
    Publisher,
    Subscriber,
    Notifier,
    Notifyee,
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RouteFamily {
    Classic,
    OwnedFrame,
    CopyMinimized,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Encoding {
    Native,
    Protobuf,
    Xcdrv2,
    Arrow,
    #[value(name = "omgidl")]
    OmgIdl,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ReliabilityArg {
    Reliable,
    BestEffort,
}

#[derive(Debug, Parser)]
#[command(version, about = "Dust DDS uProtocol role process")]
struct Args {
    /// DDS domain used for discovery and RTPS traffic.
    #[arg(long, default_value_t = 80)]
    domain_id: i32,
    /// Unique local transport identity used for self-origin suppression.
    #[arg(long)]
    origin_id: Option<String>,
    /// DDS reliability policy.
    #[arg(long, value_enum, default_value = "reliable")]
    reliability: ReliabilityArg,
    /// DDS `KEEP_LAST` history depth.
    #[arg(long, default_value_t = 32)]
    history_depth: u32,
    /// DDS carriage family.
    #[arg(long, value_enum, default_value = "classic")]
    route_family: RouteFamily,
    /// Selected payload/metadata encoding.
    #[arg(long, value_enum, default_value = "protobuf")]
    encoding: Encoding,
    /// Authority of this role.
    #[arg(long, default_value = "dds-a")]
    local_authority: String,
    /// Authority of the peer role.
    #[arg(long, default_value = "dds-b")]
    peer_authority: String,
    /// Publish or notification resource ID.
    #[arg(long, default_value_t = 0x8001)]
    topic_resource_id: u16,
    /// RPC method resource ID.
    #[arg(long, default_value_t = 0x1000)]
    method_resource_id: u16,
    /// UTF-8 payload sent by active roles and echoed by servers.
    #[arg(long, default_value = "dds-role-payload")]
    payload: String,
    /// Receive and discovery timeout.
    #[arg(long, default_value_t = 8_000)]
    timeout_ms: u64,
    /// Requested copy-minimized payload alignment.
    #[arg(long, default_value_t = 8)]
    payload_alignment: usize,
}

pub(crate) async fn run(role: Role) -> Result<(), UStatus> {
    let args = Args::parse();
    match args.route_family {
        RouteFamily::Classic => run_classic(role, &args).await,
        RouteFamily::OwnedFrame => run_owned(role, &args).await,
        RouteFamily::CopyMinimized => match args.encoding {
            Encoding::Native => {
                run_zero_copy(role, &args, up_rust::StableContainerWireFormat).await
            }
            Encoding::Protobuf => run_zero_copy(role, &args, up_rust::ProtobufWire).await,
            Encoding::Xcdrv2 => run_zero_copy(role, &args, up_wire_xcdrv2::XcdrV2Wire).await,
            Encoding::Arrow => run_zero_copy(role, &args, up_wire_arrow::ArrowWire).await,
            Encoding::OmgIdl => run_zero_copy(role, &args, up_wire_omgidl::OmgIdlWire).await,
        },
    }
}

fn config(args: &Args) -> DdsConfig {
    let mut config = DdsConfig::new(args.domain_id);
    if let Some(origin_id) = &args.origin_id {
        config.origin_id.clone_from(origin_id);
    }
    config.qos.reliability = match args.reliability {
        ReliabilityArg::Reliable => Reliability::Reliable,
        ReliabilityArg::BestEffort => Reliability::BestEffort,
    };
    config.qos.history_depth = args.history_depth;
    config
}

fn timeout(args: &Args) -> Duration {
    Duration::from_millis(args.timeout_ms)
}

fn encoding(value: Encoding) -> PayloadEncoding {
    match value {
        Encoding::Native => PayloadEncoding::RAW,
        Encoding::Protobuf => up_rust::ProtobufWire::encoding(),
        Encoding::Xcdrv2 => up_wire_xcdrv2::XcdrV2Wire::encoding(),
        Encoding::Arrow => up_wire_arrow::ArrowWire::encoding(),
        Encoding::OmgIdl => up_wire_omgidl::OmgIdlWire::encoding(),
    }
}

fn topic(authority: &str, resource_id: u16) -> Result<UUri, UStatus> {
    UUri::try_from_parts(authority, ENTITY_ID, ENTITY_VERSION, resource_id)
        .map_err(|error| invalid(format!("invalid topic URI: {error}")))
}

fn endpoint(authority: &str) -> Result<UUri, UStatus> {
    UUri::try_from_parts(authority, ENTITY_ID, ENTITY_VERSION, 0)
        .map_err(|error| invalid(format!("invalid endpoint URI: {error}")))
}

fn method(authority: &str, resource_id: u16) -> Result<UUri, UStatus> {
    UUri::try_from_parts(authority, ENTITY_ID, ENTITY_VERSION, resource_id)
        .map_err(|error| invalid(format!("invalid method URI: {error}")))
}

fn outbound_message(role: Role, args: &Args) -> Result<UMessage, UStatus> {
    let payload = args.payload.as_bytes().to_vec();
    let result = match role {
        Role::Publisher => {
            UMessageBuilder::publish(topic(&args.local_authority, args.topic_resource_id)?)
                .build_with_payload(payload, encoding(args.encoding))
        }
        Role::Notifier => UMessageBuilder::notification(
            topic(&args.local_authority, args.topic_resource_id)?,
            endpoint(&args.peer_authority)?,
        )
        .build_with_payload(payload, encoding(args.encoding)),
        Role::Client => UMessageBuilder::request(
            method(&args.peer_authority, args.method_resource_id)?,
            endpoint(&args.local_authority)?,
            u32::try_from(args.timeout_ms).unwrap_or(u32::MAX),
        )
        .build_with_payload(payload, encoding(args.encoding)),
        Role::Subscriber | Role::Notifyee | Role::Server => {
            return Err(invalid("passive role has no outbound request"));
        }
    };
    result.map_err(|error| invalid(format!("build outbound message: {error}")))
}

fn filters(role: Role, args: &Args) -> Result<(UUri, Option<UUri>), UStatus> {
    match role {
        Role::Subscriber => Ok((topic(&args.peer_authority, args.topic_resource_id)?, None)),
        Role::Notifyee => Ok((
            topic(&args.peer_authority, args.topic_resource_id)?,
            Some(endpoint(&args.local_authority)?),
        )),
        Role::Client => Ok((
            method(&args.peer_authority, args.method_resource_id)?,
            Some(endpoint(&args.local_authority)?),
        )),
        Role::Server => Ok((
            endpoint(&args.peer_authority)?,
            Some(method(&args.local_authority, args.method_resource_id)?),
        )),
        Role::Publisher | Role::Notifier => Err(invalid("active one-way role has no filter")),
    }
}

fn response_metadata(request: &UFrameMetadata, args: &Args) -> Result<UFrameMetadata, UStatus> {
    let source = request
        .sink()
        .cloned()
        .ok_or_else(|| invalid("request metadata has no sink"))?;
    UFrameMetadata::response(source, request.source().clone(), request.id().clone())
        .with_payload_encoding(encoding(args.encoding))
        .build()
        .map_err(|error| invalid(format!("build response metadata: {error}")))
}

fn response_message(request: &UMessage, args: &Args) -> Result<UMessage, UStatus> {
    let request_id = request.id().clone();
    let invoked_method = request
        .sink()
        .cloned()
        .ok_or_else(|| invalid("request has no sink"))?;
    UMessageBuilder::response(request.source().clone(), request_id, invoked_method)
        .build_with_payload(
            request
                .payload()
                .map_or_else(Vec::new, |bytes| bytes.to_vec()),
            encoding(args.encoding),
        )
        .map_err(|error| invalid(format!("build response message: {error}")))
}

struct MessageListener(mpsc::UnboundedSender<UMessage>);

#[async_trait]
impl UListener for MessageListener {
    async fn on_receive(&self, message: UMessage) {
        let _ = self.0.send(message);
    }
}

async fn run_classic(role: Role, args: &Args) -> Result<(), UStatus> {
    let transport = Arc::new(UPTransportDds::with_config(
        config(args),
        tokio::runtime::Handle::current(),
    )?);
    if matches!(role, Role::Publisher | Role::Notifier) {
        transport.wait_ready(2, timeout(args))?;
        let message = outbound_message(role, args)?;
        transport.send(message.clone()).await?;
        print_sent(role, message.payload().map_or(0, |payload| payload.len()));
        return Ok(());
    }

    let (source, sink) = filters(role, args)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    transport
        .register_listener(&source, sink.as_ref(), Arc::new(MessageListener(tx)))
        .await?;
    println!("READY listener_registered");
    if matches!(role, Role::Client) {
        transport.wait_ready(2, timeout(args))?;
        transport.send(outbound_message(role, args)?).await?;
    }
    let message = receive(&mut rx, timeout(args), "classic message").await?;
    if matches!(role, Role::Server) {
        transport.wait_ready(2, timeout(args))?;
        transport.send(response_message(&message, args)?).await?;
    }
    print_observed(role, message.payload().map_or(0, |payload| payload.len()));
    Ok(())
}

struct OwnedListener(mpsc::UnboundedSender<UOwnedFrame>);

#[async_trait]
impl UOwnedListener for OwnedListener {
    async fn on_receive_owned(&self, frame: UOwnedFrame) {
        let _ = self.0.send(frame);
    }
}

async fn run_owned(role: Role, args: &Args) -> Result<(), UStatus> {
    let transport = Arc::new(UPTransportDdsOwned::with_config(
        config(args),
        tokio::runtime::Handle::current(),
    )?);
    if matches!(role, Role::Publisher | Role::Notifier) {
        transport.wait_ready(2, timeout(args))?;
        let frame = frame_from_message(&outbound_message(role, args)?)?;
        let len = frame.payload_bytes().len();
        transport.send_owned(frame).await?;
        print_sent(role, len);
        return Ok(());
    }

    let (source, sink) = filters(role, args)?;
    verify_filter_criteria(&source, sink.as_ref()).map_err(|status| *status)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    transport
        .register_owned_listener(&source, sink.as_ref(), Arc::new(OwnedListener(tx)))
        .await?;
    println!("READY listener_registered");
    if matches!(role, Role::Client) {
        transport.wait_ready(2, timeout(args))?;
        transport
            .send_owned(frame_from_message(&outbound_message(role, args)?)?)
            .await?;
    }
    let frame = receive(&mut rx, timeout(args), "owned frame").await?;
    if matches!(role, Role::Server) {
        transport.wait_ready(2, timeout(args))?;
        transport
            .send_owned(
                UOwnedFrame::with_payload(
                    response_metadata(frame.metadata(), args)?,
                    frame.payload_bytes().to_vec(),
                )
                .map_err(|error| invalid(format!("build owned response: {error}")))?,
            )
            .await?;
    }
    print_observed(role, frame.payload_bytes().len());
    Ok(())
}

fn frame_from_message(message: &UMessage) -> Result<UOwnedFrame, UStatus> {
    let metadata = match message.payload() {
        Some(_) => {
            let encoding = message
                .attributes()
                .payload_encoding()
                .ok_or_else(|| invalid("payload-bearing message has no encoding"))?;
            message.to_frame_metadata(encoding)
        }
        None => message.to_frame_metadata_unencoded(),
    }
    .map_err(|error| invalid(format!("project frame metadata: {error}")))?;
    match message.payload() {
        Some(payload) => UOwnedFrame::with_payload(metadata, payload.to_vec()),
        None => UOwnedFrame::without_payload(metadata),
    }
    .map_err(|error| invalid(format!("build owned frame: {error}")))
}

struct ZeroCopyListener<Rx>(mpsc::UnboundedSender<Rx>);

#[async_trait]
impl<Rx> UZeroCopyListener<Rx> for ZeroCopyListener<Rx>
where
    Rx: UZeroCopyRxLease + Send + 'static,
{
    async fn on_receive_zero_copy(&self, frame: Rx) {
        let _ = self.0.send(frame);
    }
}

async fn run_zero_copy<W>(role: Role, args: &Args, wire: W) -> Result<(), UStatus>
where
    W: UWire + Copy + Send + Sync + 'static,
    NativePrefixFrameMetadataCodec: UWireMetadataCodecFor<W>,
{
    let core = DdsZeroCopyCore::with_config(config(args), tokio::runtime::Handle::current())?;
    let required_matches = usize::from(matches!(
        role,
        Role::Publisher | Role::Notifier | Role::Client
    )) + 1;
    core.wait_ready(required_matches, timeout(args))?;
    let transport = Arc::new(core.into_native_prefix_wire_transport(wire));
    if matches!(role, Role::Publisher | Role::Notifier) {
        send_zero_copy(
            &transport,
            frame_from_message(&outbound_message(role, args)?)?,
            args,
        )
        .await?;
        print_sent(role, args.payload.len());
        return Ok(());
    }

    let (source, sink) = filters(role, args)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    transport
        .register_validated_zero_copy_listener(
            &source,
            sink.as_ref(),
            Arc::new(ZeroCopyListener(tx)),
        )
        .await?;
    println!("READY listener_registered");
    if matches!(role, Role::Client) {
        send_zero_copy(
            &transport,
            frame_from_message(&outbound_message(role, args)?)?,
            args,
        )
        .await?;
    }
    let frame = receive(&mut rx, timeout(args), "copy-minimized frame").await?;
    let payload = frame.try_contiguous_payload().unwrap_or(&[]).to_vec();
    if matches!(role, Role::Server) {
        let response =
            UOwnedFrame::with_payload(response_metadata(frame.metadata(), args)?, payload.clone())
                .map_err(|error| invalid(format!("build copy-minimized response: {error}")))?;
        send_zero_copy(&transport, response, args).await?;
    }
    print_observed(role, payload.len());
    Ok(())
}

async fn send_zero_copy<T>(
    transport: &Arc<T>,
    frame: UOwnedFrame,
    args: &Args,
) -> Result<(), UStatus>
where
    T: UZeroCopyTransportImpl + Send + Sync + 'static,
    T::Tx: UTxBuffer,
{
    let mut loan = transport
        .loan_validated_tx(UTxLoanSpec::payload(
            frame.metadata().clone(),
            frame.payload_bytes().len(),
            args.payload_alignment,
        )?)
        .await?;
    loan.payload_mut().copy_from_slice(frame.payload_bytes());
    transport.send_validated_zero_copy(loan).await
}

async fn receive<T>(
    receiver: &mut mpsc::UnboundedReceiver<T>,
    duration: Duration,
    what: &str,
) -> Result<T, UStatus> {
    tokio::time::timeout(duration, receiver.recv())
        .await
        .map_err(|_| {
            UStatus::fail_with_code(
                UCode::DeadlineExceeded,
                format!("timed out waiting for {what}"),
            )
        })?
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::Unavailable, format!("{what} channel closed"))
        })
}

fn print_sent(role: Role, payload_len: usize) {
    println!(
        "FLOW sent_payload_bytes={payload_len} role={}",
        role_name(role)
    );
}

fn print_observed(role: Role, payload_len: usize) {
    println!(
        "FLOW observed_payload_bytes={payload_len} role={}",
        role_name(role)
    );
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Publisher => "publisher",
        Role::Subscriber => "subscriber",
        Role::Notifier => "notifier",
        Role::Notifyee => "notifyee",
        Role::Client => "rpc_client",
        Role::Server => "rpc_server",
    }
}

fn invalid(detail: impl Into<String>) -> UStatus {
    UStatus::fail_with_code(UCode::InvalidArgument, detail.into())
}
