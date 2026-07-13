# Follow-up: mongos/mongod version skew and why mongos-`hello` negotiation is sufficient

- **Ticket:** [DRIVERS-3454](https://jira.mongodb.org/browse/DRIVERS-3454)
- **Date:** 2026-06-08
- **Context:** Confirms that gating OP_MSG trace-context propagation on the **mongos** `hello`
  capability is safe across sharded-cluster version skew. Pairs with the stock-server rejection
  finding (`2026-06-08-stock-server-rejection.md`) and the propagation spec.

## Experiment that motivated this

Forcing the kind-3 section (`OtelTracePropagationTestToggle.FORCE_PROPAGATION=true`) against a stock
**MongoDB 7.0 sharded cluster** (config RS + shard RS + mongos) made the mongos reject **every**
command with `error 40432 (Location40432): "Unknown section kind 3"` — a clean, recoverable command
error (server stayed healthy), but a 100% app outage for those operations. This is the exact failure
the `hello`-capability gate must prevent, so it matters *which* node the driver negotiates with.

## Are mongos and mongod the same version?

Yes. `mongos` and `mongod` ship as one MongoDB Server release (same version stream), upgraded
together. The only standard divergence is **during a rolling sharded-cluster upgrade**, and the skew
direction is fixed by the required order:

```
disable balancer → config servers → shards → mongos (LAST) → re-enable balancer → bump FCV
```

So during the window the **mongos is the OLDEST** component (upgraded last); shards/config are already
newer.

### Hard constraints that bound the skew (from official docs + server source)

- *"The mongos binary cannot connect to mongod instances whose **feature compatibility version (FCV)
  is greater than that of the mongos**."* (5.0→8.0 sharded-upgrade docs.) ⇒ mongos is **never behind
  the cluster FCV**.
- During upgrade/downgrade the **internal-cluster `hello`** clamps `minWireVersion == maxWireVersion
  == LATEST` so an out-of-range internal peer fails to connect
  (`src/mongo/db/commands/feature_compatibility_version.cpp`,
  `jstests/noPassthrough/.../hello_feature_compatibility_version.js`). This clamp is for
  `internalClient` only; external app drivers still receive the full `minWireVersion=0..max` range.
- Internal TSE knowledge lists *"mongos version skew"* as a known upgrade failure mode
  (`10gen/tse-strategy-backtest-scoreboard` upgrade-paths SKILL).

**Net:** mongos may be one major behind the *binaries* transiently, but never behind the *FCV*, and
mongos ≤ shard versions during the upgrade.

## Why mongos-`hello` negotiation is sufficient

1. The driver connects to the **mongos** and reads its `hello`. Because mongos is upgraded **last**,
   it's the **lowest common denominator** the driver talks to — too-old mongos ⇒ no `tracingSupport`
   advertised ⇒ driver doesn't send the section. Conservative and safe.
2. The **mongos→shard** hop is covered: shards are upgraded **before** mongos, so once the mongos
   advertises support, the shards already support it (no risk of forwarding kind-3 to a shard that
   can't — *assuming the server re-propagates the section onward*).
3. **FCV-gating** closes the remaining gap: if advertisement is FCV-gated, the mongos won't advertise
   until FCV is bumped, which only happens after every component (incl. mongos) is upgraded ⇒ uniform
   support.

Conclusion: the driver does **not** need to probe each shard; negotiating once at the mongos is
sufficient given the documented upgrade order + FCV rule.

## Atlas Proxy / mongonet — the third (and most important) parsing surface

In Atlas the driver connects to the **Atlas Proxy**, not to mongos/mongod directly, so the proxy is
the surface that matters most.

- The Atlas Proxy uses **mongonet** for all wire-protocol parse/serialize/forward (Atlas Proxy
  Resources wiki). It fully decodes each `OP_MSG`, iterates `msg.Sections`, and re-serializes before
  forwarding.
- mongonet models sections as an **enumerated set of typed structs** (`BodySection`,
  `DocumentSequenceSection`, `SecurityToken`). The security-token section (**kind 2**) had to be
  **explicitly added to mongonet** (CLOUDP-167278 era: *"Add UnsignedSecurityToken section to
  mongonet OP_MSG"*). mongonet does **not** generically pass through arbitrary section kinds.
- **Consequence:** an un-updated Atlas Proxy will not understand **kind 3** — it will either reject it
  (most likely, mirroring mongod's `40432` → hard outage) or fail to forward it (silently broken
  propagation, section dropped before the backend). Either way Atlas needs **explicit mongonet +
  Atlas Proxy work** to parse-and-forward the section, even if mongos/mongod already support it.
- **Negotiation still works at the endpoint:** the driver's `hello` in Atlas comes *through the
  proxy*, so the proxy is the natural capability gate — it won't advertise `tracingSupport` until
  mongonet can handle the section. Same lowest-common-denominator logic as mongos-upgraded-last.

This is already flagged internally: James Kovacs's DRIVERS-3454 sync notes have an action item to
*"investigate impact on drivers and other software that parses OP_MSG… unknown section type,
especially for existing drivers/services."* Noah Stapp's **"OP_MSG Telemetry Scope"** wants the
section processable *without* server changes and notes the server must support it *"present in
potentially any message"* — plus there is an effort to **consolidate OTel context + retry telemetry +
backpressure into one generic OP_MSG telemetry section**, which may reshape this whole approach.

## Open confirmations for the server / Atlas team

1. **`hello` capability** — confirm the plan to advertise trace-context support in `hello` (e.g.
   `tracingSupport: true`); this is the production gate replacing the test toggle.
2. **Binary-gated vs FCV-gated** — will `tracingSupport` be advertised based on binary version or on
   FCV? (Affects whether the gate is uniform during the upgrade window.)
3. **mongos onward propagation** — does the mongos re-propagate the kind-3 section on the
   mongos→shard hop (so shard-side spans link), or is server-side propagation handled separately?
4. **mongonet / Atlas Proxy support** — will mongonet parse *and forward* the new section (and the
   proxy re-propagate it to the backend), so Atlas-connected drivers work rather than break?
5. **Proxy `hello` advertisement** — will the Atlas Proxy advertise `tracingSupport` (reflecting proxy
   + backend support together), so the driver's capability gate covers the Atlas path?

Items 2–5 determine whether endpoint-only negotiation (mongos or Atlas Proxy) is fully sufficient end
to end. The consolidation into a single generic telemetry section (above) may also change the design.
