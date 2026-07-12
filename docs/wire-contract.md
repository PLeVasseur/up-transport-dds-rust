# DDS Outer Wire Contract v1

`idl/up_transport_dds_v1.idl` is normative. The Rust `DdsType` structs preserve
the same field order and types. Each family has a separate topic and registered
type name, both suffixed `v1`; a future incompatible contract must use new names.

`origin_id` identifies one transport instance. A reader suppresses only samples
whose origin equals its own, so multiple Streamer instances in one process can
exchange samples without reflecting their own writes.

Classic and owned samples use `has_payload` to distinguish absent payload from
present-empty payload. Zero-copy samples additionally carry source and optional
sink as side-band routing fields. `encoded_metadata` remains opaque and is
validated by the selected-wire adapter above this core.

| Family | Topic | Registered type |
| --- | --- | --- |
| classic | `uprotocol.classic.v1` | `UpDdsClassicSampleV1` |
| owned | `uprotocol.owned.v1` | `UpDdsOwnedSampleV1` |
| behavioral zero-copy | `uprotocol.zero-copy.v1` | `UpDdsZeroCopySampleV1` |

Dust DDS 0.15 serializes these outer types using its DDS type support. The
transport does not claim a stable compact wire identity, DDS shared memory,
receive loans, native transmit loans, or end-to-end no-copy. Behavioral
zero-copy refers only to exclusive aligned transmit storage and the up-rust
witness/lease lifecycle; Dust DDS copies while serializing, writing, receiving,
and taking network samples.
