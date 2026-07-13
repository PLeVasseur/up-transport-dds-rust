# Validation and Residuals

The acceptance commands are:

```text
cargo +1.88.0 check --locked --all-targets
cargo +1.88.0 test --locked --all-targets
cargo +1.95.0 fmt --all -- --check
cargo +1.95.0 clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo +1.95.0 doc --locked --no-deps
cargo +1.95.0 test --locked --doc
cargo +1.95.0 bench --locked --no-run
cargo +1.95.0 package --locked --list
cargo deny check
```

Dust DDS 0.15 has no wait-set wake API on the synchronous reader used here.
Pollers therefore call nonblocking `take`, then wait on a condition variable;
drop wakes that condition variable and joins the poller. Discovery readiness is
the writer's DDS `PublicationMatchedStatus`, not a retry-send heuristic.

Reliable one-shot completion first observes the writer's own loopback sample,
then gives Dust DDS the remainder of the same absolute timeout budget for
matched-reader acknowledgments. Tracked writes are serialized so loopback
counts cannot be attributed to a concurrent write. `PerSend` is never retried
during drop, avoiding duplicate errors and a second timeout budget.

Dust DDS can time out an in-process acknowledgment even when the peer receives
the sample. Such bounded failures remain visible through `DdsHealth`; the
cross-process suite separately runs 100 owned Arrow RPC send/drop iterations.

Dust DDS reports malformed RTPS/XTypes input as a reader error without exposing
the rejected serialized bytes. Health therefore distinguishes invalid decoded
outer samples from DDS receive failures, but cannot attach the original malformed
network corpus to the latter.
