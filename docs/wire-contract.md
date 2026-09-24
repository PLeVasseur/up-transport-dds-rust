# DDS Outer Wire Contract v1

`idl/up_transport_dds_v1.idl` is normative. The Rust `DdsType` structs preserve
the same field order and types. Each family has a separate topic and registered
type name, both suffixed `v1`; a future incompatible contract must use new names.

`origin_id` identifies one transport instance. A reader suppresses only samples
whose origin equals its own, so multiple Streamer instances in one process can
exchange samples without reflecting their own writes.

Classic and owned samples use `has_payload` to distinguish absent payload from
present-empty payload. The copy-minimized API sample additionally carries source
and optional sink as untrusted sideband routing hints. These select physical
candidate listeners, not semantic metadata. `encoded_metadata` remains opaque;
the shared selected-wire adapter validates it and applies the final public
source/sink filter before delivery. A broad filter may receive a frame whose
advisory hint differs, but its exposed metadata is always the decoded metadata.

| Family | Topic | Registered type |
| --- | --- | --- |
| classic | `uprotocol.classic.v1` | `UpDdsClassicSampleV1` |
| owned | `uprotocol.owned.v1` | `UpDdsOwnedSampleV1` |
| copy-minimized API, owned storage | `uprotocol.zero-copy.v1` | `UpDdsZeroCopySampleV1` |

Dust DDS 0.16 serializes these outer types using its DDS type support. The Rust
types explicitly select FINAL extensibility to retain the v1 layout rather than
inherit a changed middleware default. Historical topic/type names remain v1.

| Boundary | Actual storage/copy behavior |
| --- | --- |
| Application to TX loan | Writes directly into aligned transport-owned heap storage |
| TX loan to DDS sample | Copies visible payload into the outer sample's owned vector |
| DDS publication/network/receive | DDS serialization and deserialization; no SHM data-sharing claim |
| Taken DDS sample to `DdsRxFrame` | Adopts metadata/payload vectors into `Bytes`; no extra payload copy |
| Listener fanout / application view | Shares that owned allocation and borrows slices |

The transport exposes neither native DDS transmit loans nor native receive-loan
provenance. Its sample ownership is real and remains valid through teardown, but
owning/borrowing a buffer does not prove that the middleware delivered it without
copying. The SDK native-loan capability is intentionally not implemented here.
