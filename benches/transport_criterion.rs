// SPDX-License-Identifier: Apache-2.0
//! Criterion send-stage benchmarks. Rust 1.95.0 is the benchmark authority.
//!
//! Every row uses best-effort DDS `QoS` so a fire-and-forget benchmark measures
//! loan/encode/write cost without eventually blocking on reliable local history.
//! These rows do not measure delivery, receive dispatch, or round-trip latency.

#![allow(missing_docs)]

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use up_rust::{
    EncodePayload, PayloadCodecIdentity, PayloadEncoding, UMessageBuilder, UOwnedFrame,
    UOwnedTransport, UTransport, UTxBuffer, UTxLoanSpec, UUri, UWithNativePrefixWire as _,
    UZeroCopyTransportImpl,
};
use up_transport_dds::owned::UPTransportDdsOwned;
use up_transport_dds::zero_copy::DdsZeroCopyCore;
use up_transport_dds::{DdsConfig, Reliability, UPTransportDds};

fn source() -> UUri {
    UUri::try_from_parts("dds-benchmark", 0x2_0001, 1, 0x9001).expect("valid benchmark URI")
}

fn benchmark_config(domain_id: i32) -> DdsConfig {
    let mut config = DdsConfig::new(domain_id);
    config.origin_id = format!("criterion-{domain_id}");
    config.qos.reliability = Reliability::BestEffort;
    config
}

#[allow(clippy::too_many_lines)]
fn transport_benches(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime");
    let handle = runtime.handle().clone();
    let mut group = criterion.benchmark_group("dds_best_effort_send_stages");
    group.measurement_time(Duration::from_secs(3));

    let classic = UPTransportDds::with_config(benchmark_config(30), handle.clone())
        .expect("classic transport");
    classic
        .wait_ready(1, Duration::from_secs(8))
        .expect("classic discovery");
    let message = UMessageBuilder::publish(source())
        .build_with_payload(vec![0xA5; 1024], PayloadEncoding::RAW)
        .expect("classic message");
    group.bench_function("classic_publish_1k", |bencher| {
        bencher.iter(|| {
            runtime
                .block_on(classic.send(black_box(message.clone())))
                .expect("classic publish");
        });
    });

    let owned = UPTransportDdsOwned::with_config(benchmark_config(31), handle.clone())
        .expect("owned transport");
    owned
        .wait_ready(1, Duration::from_secs(8))
        .expect("owned discovery");
    let message = UMessageBuilder::publish(source())
        .build()
        .expect("metadata message");
    let metadata = message
        .attributes()
        .to_frame_metadata(PayloadEncoding::RAW)
        .expect("frame metadata");
    let frame = UOwnedFrame::with_payload(metadata, vec![0x5A; 1024]).expect("owned frame");
    group.bench_function("owned_encode_and_publish_1k", |bencher| {
        bencher.iter(|| {
            runtime
                .block_on(owned.send_owned(black_box(frame.clone())))
                .expect("owned publish");
        });
    });

    macro_rules! selected_wire_bench {
        ($name:literal, $domain:expr, $wire:expr, $wire_ty:ty, $value:expr, $value_ty:ty) => {{
            let core = DdsZeroCopyCore::with_config(benchmark_config($domain), handle.clone())
                .expect("zero-copy core");
            core.wait_ready(1, Duration::from_secs(8))
                .expect("zero-copy discovery");
            let transport = core.into_native_prefix_wire_transport($wire);
            let value: $value_ty = $value;
            let layout = <$wire_ty as EncodePayload<$value_ty>>::payload_layout(&value)
                .expect("payload layout");
            let message = UMessageBuilder::publish(source())
                .build()
                .expect("metadata message");
            let metadata = message
                .attributes()
                .to_frame_metadata(<$wire_ty as PayloadCodecIdentity>::encoding())
                .expect("payload encoding");
            group.bench_function($name, |bencher| {
                bencher.iter(|| {
                    runtime.block_on(async {
                        let mut loan = transport
                            .loan_validated_tx(
                                UTxLoanSpec::payload(
                                    black_box(metadata.clone()),
                                    layout.len(),
                                    layout.align(),
                                )
                                .expect("loan spec"),
                            )
                            .await
                            .expect("loan");
                        <$wire_ty as EncodePayload<$value_ty>>::encode_payload(
                            black_box(&value),
                            loan.payload_mut(),
                        )
                        .expect("payload encode");
                        transport
                            .send_validated_zero_copy(loan)
                            .await
                            .expect("zero-copy publish");
                    });
                });
            });
        }};
    }

    selected_wire_bench!(
        "copy_minimized_xcdrv2_loan_encode_publish",
        32,
        up_wire_xcdrv2::XcdrV2Wire,
        up_wire_xcdrv2::XcdrV2Wire,
        up_wire_xcdrv2::VEHICLE_SIGNAL_V1_GOLDEN_VALUE,
        up_wire_xcdrv2::VehicleSignalV1
    );
    selected_wire_bench!(
        "copy_minimized_arrow_loan_encode_publish",
        33,
        up_wire_arrow::ArrowWire,
        up_wire_arrow::ArrowWire,
        up_wire_arrow::TelemetryTableV1::fixture(4096, 7),
        up_wire_arrow::TelemetryTableV1
    );
    selected_wire_bench!(
        "copy_minimized_omgidl_loan_encode_publish",
        34,
        up_wire_omgidl::OmgIdlWire,
        up_wire_omgidl::OmgIdlWire,
        up_wire_omgidl::VehicleStatusV1::fixture(42),
        up_wire_omgidl::VehicleStatusV1
    );

    group.finish();
}

criterion_group!(benches, transport_benches);
criterion_main!(benches);
