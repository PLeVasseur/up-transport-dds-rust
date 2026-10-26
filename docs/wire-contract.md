# DDS Outer Wire Contract v1

`idl/up_transport_dds_v1.idl` is normative. The Rust `DdsType` structs preserve
the same field order and types. Each family has a separate topic and registered
type name, both suffixed `v1`; a future incompatible contract must use new names.

`origin_id` identifies one transport instance. A reader suppresses only samples
whose origin equals its own, so multiple Streamer instances in one process can
exchange samples without reflecting their own writes.

Classic and owned samples use `has_payload` to distinguish absent payload from
present-empty payload. The encoded-owned sample additionally carries source
and optional sink as untrusted sideband routing hints. These select physical
candidate listeners, not semantic metadata. `encoded_metadata` remains opaque;
the shared selected-wire adapter validates it and applies the final public
source/sink filter before delivery. A broad filter may receive a frame whose
advisory hint differs, but its exposed metadata is always the decoded metadata.

Classic reconstruction projects the received attributes' own optional encoding
with `UAttributes::try_project_to_frame_metadata`, then checks it against the
sample's explicit payload presence when constructing the owned frame. Complete
messages use the checked message projection. Neither path overrides a received
encoding or conflates absent with present-empty data. Selected-wire metadata
contexts combine wire/family identity with the configured codec's layout identity.
The canonical SDK codec is `UFrameFieldsMetadataCodec`; changing its API name does
not change the outer DDS topic, type or metadata bytes.

| Family | Topic | Registered type |
| --- | --- | --- |
| classic | `uprotocol.classic.v1` | `UpDdsClassicSampleV1` |
| owned | `uprotocol.owned.v1` | `UpDdsOwnedSampleV1` |
| selected-wire encoded owned | `uprotocol.encoded-owned.v1` | `UpDdsEncodedOwnedSampleV1` |

Dust DDS 0.16 serializes these outer types using its DDS type support. The Rust
types explicitly select FINAL extensibility to retain the v1 layout rather than
inherit a changed middleware default. The encoded-owned topic/type are new names;
classic and validated-owned retain their original v1 names and bytes.

| Boundary | Actual storage/copy behavior |
| --- | --- |
| Application to SDK wire adapter | Sends a validated `UOwnedFrame`; adapter prepares selected-wire metadata |
| Prepared owned frame to DDS sample | Moves metadata, copies payload into the outer sample's owned vector |
| DDS publication/network/receive | DDS serialization and deserialization; no SHM data-sharing claim |
| Taken DDS sample to `EncodedOwnedFrame` | Moves metadata vector, adopts payload vector into `Bytes` |
| Listener fanout / application view | Clones owned metadata and shares owned payload allocation |

## Capability and migration

`DdsEncodedOwnedCore` implements `UOwnedTransportCore`. It does not implement
`PreparedTransportCore`; a compile-time negative assertion pins that distinction.
Dust DDS 0.16.0's evaluated writer/reader path uses
owned samples and mandatory serialization. Native DDS sample-pool ownership,
writer/reader loans, shared-memory data-sharing and their lifetime/allocation proof
would be prerequisites for a separate future native capability.

The former `zero_copy` module, `DdsZeroCopyCore`, transmit-loan buffers and
`copy-minimized` CLI family are replaced, without zero-copy aliases. Select
`encoded_owned::DdsEncodedOwnedCore` and `--route-family encoded-owned` instead.
The obsolete alignment CLI setting is rejected. The new topic remains physically
distinct from classic and validated-owned topics; mapping both owned APIs onto
one topic would permit unintended direct delivery around a bridge.

Historical `uprotocol.zero-copy.v1` / `UpDdsZeroCopySampleV1` traffic is not accepted
on the new topic. Deployment peers must migrate together and configure the
encoded-owned API explicitly.

The standalone encoded-owned role binaries configure serialized wires. They reject
`--encoding native` before DDS startup because their text payload has neither an
explicit native-profile agreement nor a declared stable representation. Classic
and owned-frame roles keep that option's raw-byte meaning. Direct SDK adapter users
may supply an explicit agreement for opaque native bytes in owned storage; this
is still encoded-owned DDS carriage, not a middleware-native loan.

```rust
use up_rust::{UOwnedTransport, UStatus, UWithNativePrefixWire};
use up_transport_dds::encoded_owned::DdsEncodedOwnedCore;

fn configure(runtime: tokio::runtime::Handle) -> Result<impl UOwnedTransport, UStatus> {
    let core = DdsEncodedOwnedCore::new(80, runtime)?;
    Ok(core.into_protobuf_transport())
}
```

## Ownership, filtering and shutdown

Returned owned frames remain usable after unregister or transport destruction.
That ownership guarantee does not imply native loans or middleware no-copy delivery.
Encoded sideband hints select candidates only; the SDK decodes metadata, checks
selected-wire identity and applies the public filter before user delivery.
`sink_filter=None` means sinkless. Private wire/family identities require literals;
compact-only matching belongs only to registered first-wave identities. Payload
encoding identifiers are a separate namespace from wire/family/layout identities.

The core preserves the binding's existing bounded dispatch queue, wakeable poll,
in-flight listener removal, health reporting, joined shutdown and domain restart
contracts. `wait_ready` reports DDS reader matching; `wait_acknowledged` reports
prior writes acknowledged by currently matched reliable readers, not callback
completion. The SDK's optional native quiescence extension is not a claim that
this owned binding acquired native sample loans.
