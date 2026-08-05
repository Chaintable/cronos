# Continuous block sync mode implementation plan

Status: Implemented; soak test and production rollout pending

Implementation:

- CometBFT: [Chaintable/cometbft#1](https://github.com/Chaintable/cometbft/pull/1),
  commit `1940ba94e3ef084ec16d9d6597122a176d463e8c`.
- Cronos: [Chaintable/cronos#9](https://github.com/Chaintable/cronos/pull/9),
  pinning the CometBFT commit above.

## 1. Decision

Add an opt-in `continuous` block sync mode for dedicated, non-validator writer
nodes. Keep the existing CometBFT behavior as the default:

```toml
[blocksync]
version = "v0"
mode = "auto" # auto | continuous
```

- `auto`: unchanged behavior. Once block sync catches up, stop the block pool
  and enter consensus.
- `continuous`: start in block sync and keep the block pool running at the
  chain tip. Do not enter consensus while the local private-validator address
  has no voting power.

This first version is a startup mode, not a runtime
`consensus -> blocksync -> consensus` switch. Dynamic re-entry remains out of
scope because the current block pool and consensus state machines are not
restartable after they have been stopped.

## 2. Why this cannot be implemented only in the Cronos application layer

Cronos passes `tmcfg.DefaultConfig()` to the Cosmos SDK server in
`cmd/cronosd/cmd/root.go`. The current `[blocksync]` configuration comes from
the pinned CometBFT module and only contains `version`.

The mode transition itself is also owned by CometBFT:

- `blocksync/pool.go`: `IsCaughtUp` becomes true when the block pool reaches
  the highest peer height minus one. Block `H+1` is required to verify block
  `H`'s commit.
- `blocksync/reactor.go`: when caught up, the reactor stops the block pool and
  calls `SwitchToConsensus`.
- `blocksync/reactor.go`: `SwitchToBlockSync` only implements the one-time
  handoff after state sync; it does not make a stopped block pool restartable.
- `rpc/core/status.go`: `catching_up` is derived from the consensus reactor's
  `WaitSync` state.

Before this change, the Cronos module replaced CometBFT with
`github.com/crypto-org-chain/cometbft@178ea8502144`, and there was no
`Chaintable/cometbft` repository. The implementation uses two reviewable PRs:

1. A standalone public `Chaintable/cometbft` fork based exactly on
   `178ea8502144`, containing the configuration and reactor change.
2. This `Chaintable/cronos` repository, pinning that fork by immutable commit
   and publishing the Cronos image.

Vendoring the complete CometBFT source into this repository is rejected: the
current build uses `-mod=readonly`, and carrying a full copied dependency would
make upstream rebases and security updates substantially harder to review.

## 3. CometBFT change

### 3.1 Configuration

Extend `config.BlockSyncConfig` with `Mode string`:

- Default: `auto`.
- Accepted values: `auto`, `continuous`.
- Any other value fails `ValidateBasic` at startup.
- Add the field and its comments to the generated `config.toml` template.
- Existing `config.toml` files that omit `mode` retain `auto` behavior through
  the default config object.

Use one option in the first version. In `continuous` mode, use a one-second
peer status refresh interval; retain the existing ten-second interval in
`auto` mode. A separate tuning option is unnecessary until measurements show
that one second is unsuitable.

### 3.2 Reactor behavior

Pass the configured mode through `node/setup.go` into the block sync reactor.
In `blocksync/reactor.go`:

1. Start the block pool exactly as today.
2. Continue applying and committing verified blocks through the existing
   `BlockExecutor` path.
3. When `IsCaughtUp()` becomes true:
   - in `auto`, retain the current stop-and-switch behavior;
   - in `continuous`, keep the pool and its request routine running.
4. At the tip, broadcast `StatusRequest` every second so new peer heights are
   discovered without the current ten-second staircase delay.
5. Rate-limit the "caught up, waiting for peer height" log so the one-second
   switch ticker does not generate repetitive logs.
6. Preserve normal shutdown: stopping the node still stops the block pool and
   waits for its goroutines.

Do not change `BlockPool.IsCaughtUp()` or block verification. Continuous mode
will therefore normally stay at least one block behind the highest peer; this
is a correctness requirement, not a lag bug.

### 3.3 Validator safety

Continuous mode is only valid for a dedicated non-validator node:

- During startup, reject `continuous` if the local private-validator address
  already has positive voting power in the loaded validator set.
- After every applied block, if that address gains positive voting power,
  switch to consensus at the normal safe transition point and emit an error
  log/metric. This protects the chain if governance later adds the node to the
  validator set.
- The production writer must not share a validator key with any validator.

The existing `localNodeBlocksTheChain` check only handles voting power of at
least one third. That threshold is insufficient for this mode because even a
smaller validator would miss votes permanently.

### 3.4 RPC and observability semantics

Keep standard CometBFT semantics:

- `/status.sync_info.catching_up` remains `true` in continuous mode because
  consensus is deliberately waiting. Do not redefine this field to mean
  "height lag is healthy".
- Read RPC continues to use the committed `BlockStore.Height()` while syncing.
- The deployment must not use `catching_up == false` as readiness. Readiness
  and alerts should compare local block height/time against a trusted reference.
- Add gauges for the maximum reported peer height and whether continuous mode
  is enabled. Existing `blocksync_latest_block_height` remains the local
  progress metric.

The mode is intended for the DeBank pipeline writer. It must not be advertised
as a public transaction-ingress or validator endpoint. Read RPC, including
`trace_debankBlock`, remains in scope.

## 4. Cronos repository change

The Cronos integration:

1. Update the `go.mod` replace target from `crypto-org-chain/cometbft` to the
   exact `Chaintable/cometbft` commit and refresh `go.sum`.
2. Add release notes documenting `blocksync.mode`, the non-validator
   restriction, permanent `catching_up=true`, and the expected one-or-more
   block lag.
3. Build the existing `Dockerfile.debank-rocksdb` image and pin the resulting
   immutable tag/digest in the deployment PR.
4. Do not change the default mode for validators, RPC replicas, seeds, or
   existing installations. Only the Cronos writer deployment opts into
   `continuous`.

The first version enables the mode through `config.toml`. A separate CLI flag
or environment-variable override is unnecessary until the deployment layer
has a concrete need for it.

## 5. Tests

### 5.1 CometBFT unit tests

- `config/config_test.go`: default is `auto`; accept both modes; reject unknown
  modes.
- `config/toml_test.go`: generated config includes `mode` and round-trips it.
- `blocksync/reactor_test.go`:
  - `auto` still stops the pool and starts consensus when caught up;
  - `continuous` remains in block sync after catching up;
  - after a peer advertises a new height, a caught-up continuous node resumes
    downloading without restart;
  - node shutdown stops all block sync goroutines;
  - a local validator is rejected at startup;
  - a node that gains voting power leaves continuous mode safely.
- Metrics test: peer height and mode gauges have stable names and values.

Run the CometBFT package tests with the Go version declared by that fork.

### 5.2 Cronos build and integration tests

Run at minimum:

```bash
go mod verify
make test
make build
docker build -f Dockerfile.debank-rocksdb .
```

Completed local checks:

- CometBFT: related package tests, targeted race test, and `go vet` passed.
- Cronos: `go mod verify`, command package tests, `make build`, and `make test`
  passed.
- A fresh `cronosd init` generated `[blocksync] mode = "auto"`.

The RocksDB Docker image build and the following integration/soak tests remain
release gates.

Use a local three-node Cronos network:

- one validator in normal consensus mode;
- one normal non-validator control node;
- one writer in continuous block sync mode.

Verify:

1. The writer reaches the tip and never logs `SwitchToConsensus`.
2. Stop its P2P connectivity for five minutes, restore it, and confirm it
   catches up faster than chain production without a process restart.
3. Disconnect the best peer and confirm another block-serving peer takes over.
4. Halt block production and confirm there is no restart or busy-log loop.
5. Query historical and near-tip `eth_getBlockByNumber` and
   `trace_debankBlock`; compare block hash and DeBank payload with the control
   node.
6. Run the writer plus ETL and confirm the ETL checkpoint continues to advance
   without gaps or duplicate output.

## 6. Acceptance criteria

On `lihe-dev`, with at least two stable block-serving persistent peers:

- 24-hour soak test with zero writer restarts, panic, OOM, or database recovery.
- At steady state, p95 lag is at most five blocks and maximum sustained lag is
  below 30 seconds when the reference node continues advancing.
- After a five-minute network interruption, the writer re-enters steady-state
  lag without restart and its catch-up rate is higher than the live chain rate.
- CPU and disk utilization do not regress materially versus startup block sync;
  peer status traffic remains bounded at one request per connected peer per
  second.
- `trace_debankBlock` samples and ETL output match the normal control node.

These targets must be measured; they are release gates, not assumptions.

## 7. Rollout and rollback

1. Publish a test image pinned to the Cronos and CometBFT commits.
2. Run the local integration and 24-hour `lihe-dev` soak tests.
3. Have SRE deploy one production writer canary with `mode=continuous`, stable
   internal persistent peers, and height/time-lag monitoring.
4. Observe for 24 hours before expanding to other non-validator writers.

Rollback is configuration-only (`mode=auto`) or image rollback. There is no
database migration and both modes use the same block execution and storage
path. Production changes are executed by SRE; this repository only supplies
the tested image and runbook.

## 8. Known limitation

Continuous mode prevents the specific slow path where a far-behind writer has
already left block sync and can only consume historical commits at consensus
speed. It does not repair a block pool that has no usable block-serving peers.
Stable internal persistent peers and peer-lag monitoring remain required.

If a future requirement is specifically "switch back to block sync only after
lag exceeds five minutes", implement that separately. The preferred first
fallback remains a guarded graceful process restart; an in-process dynamic
transition requires reset/reconstruction support for the block pool, consensus
state, WAL, and mempool.
