# up-transport-dds

`up-transport-dds` is a standalone Rust 1.88 physical transport for Eclipse
uProtocol using Dust DDS 0.16 discovery and RTPS. It provides three separately
versioned carriage families:

- `UPTransportDds`: classic `UMessage` carriage with explicit payload presence.
- `UPTransportDdsOwned`: canonical validated owned-frame carriage.
- `encoded_owned::DdsEncodedOwnedCore`: selected-wire encoded-owned carriage
  implementing `UOwnedTransportCore`, composed with the SDK's owned wire adapter.

**DDS implements owned APIs, not the zero-copy trait family.** Dust DDS 0.16
serializes owned samples and returns owned decoded samples; it exposes no native
TX/RX loan or shared-memory data-sharing API on this path. Send copies owned
payload bytes into the middleware sample. Receive adopts the payload vector into
`Bytes`; fanout shares payload storage and clones owned metadata. There are no
initialized/uninitialized loan methods or compatibility aliases claiming those
capabilities. See the rendered, compile-tested [wire and capability guide](docs/wire-contract.md).
Classic/owned reconstruction uses checked SDK projections and preserves the
sample's payload-presence distinction. Selected-wire contexts take layout identity
from the configured metadata codec; physical topic/type contracts stay unchanged.

Genuine DDS zero-copy requires middleware writer/reader loans, data-sharing
delivery and suitable plain, bounded types. The current v1 envelope's unbounded
strings and sequences are not such a type. That capability would require a new
carriage profile and Rust binding to a supporting middleware. Use the encoded-owned
API for the currently implemented selected-wire path.

## Configuration

`DdsConfig` makes the DDS domain, exact origin identity, reliability, history
depth, polling interval, per-take limit, and bounded callback queue explicit.
Each instance suppresses only samples carrying its own origin. `wait_ready`
uses DDS `PublicationMatchedStatus`; applications do not need retry sends to
cover discovery.

For reliable terminal sends, call `wait_acknowledged(timeout)` after writing and
before dropping the producer. A successful write queues data; publication matching
alone does not establish delivery completion. The bounded acknowledgement barrier
waits for previously written samples at currently matched reliable readers, not
future discovery or application callbacks. Best-effort writers return immediately.
Transport send methods keep their existing local-write semantics.

Background work is bounded and observable through `DdsHealth`. Dropping a
transport wakes and joins its poller, cancels callback work, drains the bounded
dispatcher, and deletes the participant's contained entities. Successful
listener unregistration waits for an in-flight callback and prevents any
snapshotted-but-not-started callback from running afterward.

## Role Binaries

Six user-facing processes provide machine-readable `READY` and `FLOW` output:

```text
dds_publisher  dds_subscriber
dds_notifier   dds_notifyee
dds_client     dds_server
```

Every binary accepts `--domain-id`, `--origin-id`, `--reliability`,
`--history-depth`, `--route-family classic|owned-frame|encoded-owned`, and
`--encoding native|protobuf|xcdrv2|arrow|omgidl`. For example:

```text
cargo run --bin dds_subscriber -- --domain-id 80 --origin-id subscriber \
  --route-family encoded-owned --encoding arrow \
  --local-authority dds-b --peer-authority dds-a
cargo run --bin dds_publisher -- --domain-id 80 --origin-id publisher \
  --route-family encoded-owned --encoding arrow \
  --local-authority dds-a --peer-authority dds-b
```

Start the passive role first and wait for `READY listener_registered`. The
active role waits for the remote DDS reader and sends once.
The old `copy-minimized` family and `--payload-alignment` loan setting are removed;
unsupported old options fail explicitly. Encoded-owned uses its own DDS topic,
not the validated-owned topic, so distinct physical families cannot bypass a bridge.
`--encoding native` carries raw bytes in the classic and owned-frame role paths.
The encoded-owned role path rejects that selection before creating a DDS endpoint:
it has no explicit native-profile agreement or declared stable type for the text
`--payload`. It does not synthesize a profile or fall back to another wire.
Applications using the SDK adapter directly can supply an explicit native agreement
for opaque owned carriage; that does not create a native DDS loan capability.

## Wire Dependencies

`Cargo.toml` and `Cargo.lock` specify the SDK and codec dependencies; use
`--locked` for reproducible builds. Arrow and OMGIDL are selected-wire
codecs, not physical transports. Their metadata and payload remain opaque in
the encoded-owned DDS core; source and optional sink routing travel in
structurally checked outer routing hints. The shared SDK adapter decodes metadata
and applies the public source/sink filters; hints never override that metadata.

## Validation

Public documentation uses `missing_docs = "deny"`. Compile-time assertions require
the owned core and reject both zero-copy core traits. Real RTPS tests cover selected
wire filtering, presence, retained owned frames, physical isolation and teardown.

```text
cargo +1.88.0 check --locked --all-targets
cargo +1.88.0 test --locked --all-targets
cargo +1.95.0 fmt --all -- --check
cargo +1.95.0 clippy --locked --all-targets -- -D warnings
cargo +1.95.0 test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo +1.95.0 doc --locked --no-deps
cargo +1.95.0 test --locked --doc
cargo +1.95.0 bench --locked --no-run
cargo +1.95.0 package --locked --list
cargo deny check advisories licenses bans sources
```

The integration suite uses distinct Dust DDS participants and includes
cross-process classic pub/sub, owned notification, and Arrow RPC over RTPS.
Criterion rows separate classic publication, owned encode/publication, and
selected-wire owned encode/publication. They use explicit best-effort QoS to
measure nonblocking send-stage cost; they are not delivery, receive, or RTT
claims. Rust 1.95.0 is the current and benchmark authority; Rust 1.88.0 remains
the MSRV.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
