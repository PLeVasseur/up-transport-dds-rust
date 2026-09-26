# up-transport-dds

`up-transport-dds` is a standalone Rust 1.88 physical transport for Eclipse
uProtocol using Dust DDS 0.16 discovery and RTPS. It provides three separately
versioned carriage families:

- `UPTransportDds`: classic `UMessage` carriage with explicit payload presence.
- `UPTransportDdsOwned`: canonical validated owned-frame carriage.
- `DdsZeroCopyCore`: selected-wire/copy-minimized API adapter with owned storage.

**The DDS path is not native zero-copy.** Its historical type name identifies the
SDK's loan-style API family. TX uses aligned heap storage and copies it into an
owned DDS sample. Dust DDS 0.16 serializes that sample and returns owned decoded
samples on RX; it exposes no native TX/RX loan or shared-memory data-sharing API.
The receive vectors are adopted into `Bytes` without another payload copy, and
listener fanout shares that owned allocation. See `docs/wire-contract.md`.

Genuine DDS zero-copy requires middleware writer/reader loans, data-sharing
delivery and suitable plain, bounded types. The current v1 envelope's unbounded
strings and sequences are not such a type. That capability would require a new
carriage profile and Rust binding to a supporting middleware. Passing a
`dds-copy-minimized` integration row proves API/routing behavior, not native
zero-copy. Streamer bridge copying is a separate boundary as well.

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

Six user-facing processes provide Streamer-style `READY` and `FLOW` evidence:

```text
dds_publisher  dds_subscriber
dds_notifier   dds_notifyee
dds_client     dds_server
```

Every binary accepts `--domain-id`, `--origin-id`, `--reliability`,
`--history-depth`, `--route-family classic|owned-frame|copy-minimized`, and
`--encoding native|protobuf|xcdrv2|arrow|omgidl`. For example:

```text
cargo run --bin dds_subscriber -- --domain-id 80 --origin-id subscriber \
  --route-family copy-minimized --encoding arrow \
  --local-authority dds-b --peer-authority dds-a
cargo run --bin dds_publisher -- --domain-id 80 --origin-id publisher \
  --route-family copy-minimized --encoding arrow \
  --local-authority dds-a --peer-authority dds-b
```

Start the passive role first and wait for `READY listener_registered`. The
active role waits for the remote DDS reader and sends once.

## Wire Dependencies

The standalone lock resolves one graph from these exact public revisions:

| Dependency | Revision |
| --- | --- |
| up-rust | `d7f50d06ecdbd3f6631745e2514727bc422498a2` |
| XCDRv2 | `d5dababaaa6cc842f4b56b6ba897cf833128e42b` |
| Arrow | `e382a5500fce88fbc049dbef58afa28f9bb64fcf` |
| OMGIDL | `c8dd23ee886b0f65ed645528f492a828b2d90bd4` |

All resolved Git sources use public HTTPS and no resolved package is a sibling
path dependency. Arrow and OMGIDL are selected-wire
codecs, not physical transports. Their metadata and payload remain opaque in
the copy-minimized DDS core; source and optional sink routing travel in
structurally checked outer routing hints. The shared SDK adapter decodes metadata
and applies the public source/sink filters; hints never override that metadata.

## Validation

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
selected-wire loan/encode/publication. They use explicit best-effort QoS to
measure nonblocking send-stage cost; they are not delivery, receive, or RTT
claims. Rust 1.95.0 is the current and benchmark authority; Rust 1.88.0 remains
the MSRV.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
