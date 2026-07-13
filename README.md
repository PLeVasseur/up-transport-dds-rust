# up-transport-dds

`up-transport-dds` is a standalone Rust 1.88 physical transport for Eclipse
uProtocol using Dust DDS 0.15 discovery and RTPS. It provides three separately
versioned carriage families:

- `UPTransportDds`: classic `UMessage` carriage with explicit payload presence.
- `UPTransportDdsOwned`: canonical validated owned-frame carriage.
- `DdsZeroCopyCore`: behavioral zero-copy loans and immutable receive leases.

The third family is copy-minimized only at the uProtocol API boundary. Dust DDS
0.15 does not expose native transmit loans, receive loans, shared memory, or an
end-to-end no-copy path. DDS serialization, publication, receive, and sample
take copy bytes. See `docs/wire-contract.md` for the exact contract.

## Configuration

`DdsConfig` makes the DDS domain, exact origin identity, reliability, history
depth, polling interval, per-take limit, bounded callback queue, and reliable
delivery-completion policy explicit. `AcknowledgmentMode::OnDrop` uses one
bounded budget to flush send-once transports during teardown, `PerSend`
returns completion failures from `send`, and `Disabled` is fire-and-forget.
Each instance suppresses only samples carrying its own origin. `wait_ready`
uses DDS `PublicationMatchedStatus`; applications do not need retry sends to
cover discovery.

Background work is bounded and observable through `DdsHealth`. Dropping a
transport wakes and joins its poller, cancels callback work, drains the bounded
dispatcher, and deletes the participant's contained entities. Successful
listener unregistration waits for an in-flight callback and prevents any
snapshotted-but-not-started callback from running afterward.
`DdsHealth` retains teardown completion failures after the transport is gone;
`PerSend` failures are both returned and recorded exactly once.

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
| up-rust | `31ee659aae9588fabfd99678730a8229a0cdc18c` |
| XCDRv2 | `27a6451ac6f46b2f30cebb82b5f0ada04eb78146` |
| Arrow | `0f064f049124f3dce9d2413fcb73cf05e87d8f35` |
| OMGIDL | `280378616ade087340c89c5c65237ed14a32eaaf` |

All resolved Git sources use public HTTPS and no resolved package is a sibling
path dependency. Arrow and OMGIDL are selected-wire
codecs, not physical transports. Their metadata and payload remain opaque in
the copy-minimized DDS core; source and optional sink routing travel in
validated outer sideband fields.

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
cross-process classic pub/sub, owned notification, and 100 immediate-drop
owned Arrow RPC iterations over RTPS.
Criterion rows separate classic publication, owned encode/publication, and
selected-wire loan/encode/publication. They use explicit best-effort QoS to
measure nonblocking send-stage cost; they are not delivery, receive, or RTT
claims. Rust 1.95.0 is the current and benchmark authority; Rust 1.88.0 remains
the MSRV.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
