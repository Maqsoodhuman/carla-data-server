# Lab Status

This file is updated by the Lab PC Claude Code instance after each run.
Laptop Claude reads this to understand what happened and what to fix/improve.

---

## Last Updated
2026-09-21 00:20 EDT (session paused here — operator closing down for the day)

## Environment
- CARLA version: 0.9.16
- Lab PC IP (LAN/WAN, campus wifi blocks inbound TCP to this): 128.205.222.211
- Lab PC also has a live Tailscale interface — confirmed genuinely reachable
  from the laptop this session (see "Cross-machine" below). Exact tailnet
  IPs/hostnames deliberately omitted from this file since it's committed to a
  public repo (GitHub's own push classifier flagged an earlier attempt to
  include them).
- Map loaded on both simulators: UBAutonomousProvingGrounds
- Server tick rate: 20 Hz
- Two CARLA instances were run simultaneously on this box for the `mirror`
  scenario: primary on 2000 (+2001/+2002 streaming), shadow on
  `-carla-rpc-port=2003` (+2004/+2005). GPU: RTX 5090, 32GB — only ~4GB used
  running both, no contention.

---

## What Was Run (this session, in order)
1. `git pull origin main`: `1a2350b` → `5e86ea4` → `897fce0` → `24f2571`
   (orchestration package → map-verify → mirror/shadow-port fix + mailbox →
   auto-sync + two more bug fixes + hermetic doctor tests)
2. `doctor --role lab`, CARLA headless + map load, `pytest tests/ -q`
3. Full 5-scenario suite, both workers on this machine (proof run before
   touching the laptop) — see prior results below
4. Committed + pushed `lab-status.md` (redacted); merged cleanly with a
   same-minute fix pushed independently from the laptop side
5. Pulled the mailbox feature (`897fce0`), used
   `orchestration mail send/read` to coordinate with the laptop's agent
   instead of the human relaying text
6. Re-verified `mirror`: SKIPPED correctly with only a TCP probe (no shadow
   present), then stood up a real second CARLA on 2003 + traffic
   (`generate_traffic.py -n 15`, 6 vehicles settled) and got a genuine PASS
7. Pulled `24f2571` (two bug fixes + hermetic doctor tests): confirmed
   `pytest tests/ -q` → **118 passed**, 0 failed
8. Enabled `--auto-sync` (operator approved) — immediately surfaced the
   **first real cross-machine run**, unprompted (see below), then paused
   for the day per the "only run while watching" guidance

---

## Results / Observations

### doctor --role lab
Fully green after CARLA + map load, including the map-verify check. `websockets`
16.0 in `venv/` still has server.py's legacy API (deprecated, not removed) — no
downgrade needed here.

### pytest tests/ -q
First run: **87 passed, 1 failed** (`test_check_is_skipped_not_failed_when_map_cannot_be_read`
— assumed CARLA unreachable, but this venv legitimately has it, so the check
returned `ok: True`, flipping the assertion). Reported to the laptop side, who
fixed it in `24f2571` by making the doctor tests hermetic (stub the map read
instead of relying on CARLA's ambient absence). Re-ran after pulling the fix:
**118 passed, 0 failed.**

### Scenario suite (same-machine proof run, real CARLA)
| Scenario | Result |
|---|---|
| connectivity | PASS |
| world_state | PASS |
| sustained_stream | PASS (20.1s, no drops) |
| reconnect | PASS — real `restart_data_server`, client reconnected with a new session |
| mirror | ERROR → fixed upstream → SKIPPED (no shadow) → **PASS** (real shadow + real traffic) |

`mirror` history: first attempt ERRORed because a lone CARLA's own streaming
socket (rpc_port+1 = 2001) collided with the default `SHADOW_CARLA_PORT`, so
the TCP-only precondition check saw "reachable" and the scenario tried a real
RPC handshake against a non-RPC port and timed out. Laptop side fixed this in
`adf3660`/earlier (handshake is now the precondition, default shadow port
moved to 2003, docs updated). Re-verified: SKIPPED correctly (`"nothing
listening at 127.0.0.1:2003"`) when no shadow was up. Then stood up a real
second CARLA on 2003 with the map loaded and traffic in the primary sim, and
got a genuine PASS: `shadow_map_matches` ✓, `bridge_stayed_up` ✓ (exit=-15,
expected SIGTERM at scenario end), `shadow_actors_appeared` ✓ (0 → 6). This is
the first time `mirror` has actually exercised cross-simulator replication
rather than a precondition path.

### Cross-machine (first real occurrence — unplanned, happened on its own)
After enabling `--auto-sync` and restarting the lab worker with
`--keep-alive`, the very first advertised run was claimed within ~1s by
`client-maqsood-Alienware-m17-48603` — the laptop's own client worker,
already polling continuously over Tailscale, not anything started from this
side. That makes this the first genuine cross-machine run (not a same-box
proof run):
- `connectivity` — **PASS**, cross-machine, over Tailscale
- `world_state` — **FAIL** on `unsubscribed_topics_absent`:
  ```
  expected: no ['sensors', 'traffic_lights'] keys (server-side per-client filtering)
  observed: leaked ['sensors', 'traffic_lights']
  ```
  Client subscribed only to `vehicles`/`pedestrians` but received the other
  topics' keys anyway. All other `world_state` assertions passed (message
  validity, tick/sim-time monotonicity, subscribed topics present). Full
  result: `.orchestration/runs/run-20260921-001620-764968.json`.
  Suite halted there (`on-failure=stop`, the default).

This is a genuine, previously-unseen bug — the same-machine proof run earlier
this session had `world_state` PASS cleanly. It only showed up with a
different, independently-connecting client. Not investigated further before
pausing; best guess, unconfirmed: `BroadcastThread`'s per-`(subscriptions,
ego)` JSON-encoding cache (see `CLAUDE.md`'s architecture section) may be
keying or reusing a cached encoding across two clients with different
subscription sets, rather than the topic filter itself being wrong — worth
checking `_filter`'s cache-key construction first.

### Mailbox (`orchestration mail send/read`, added `897fce0`)
Used this instead of the human relaying text for most of this session's
back-and-forth. Full transcript is in `.orchestration/messages.json` on this
machine (durable, ordered, not repeated verbatim here). Topics covered:
mirror fix confirmation, pytest fix confirmation, the `_format_summary`
NameError bug (reported by lab, fixed by laptop in `adf3660`), a client-worker
startup-ordering bug (found+fixed by laptop while fixing the above), and the
auto-sync proposal/pause discussed below.

### Auto-sync (`--auto-sync`, added `adf3660`, enabled this session)
Enabled once, briefly, with operator approval. Confirmed: coordinator prints
an explicit warning on startup (`no authentication... only do this on a
trusted network`). Immediately produced the first real cross-machine run
(above) — the laptop's client was apparently already polling this
coordinator's address before I even enabled it, per its own mailbox message.
Paused (lab worker stopped, `Ctrl`+`C`-equivalent) at the end of this session
per the "only run while watching" guidance, since the operator is stepping
away for the day and the coordinator has no auth. **Auto-sync is currently
OFF.** No auto-sync retry/re-pull cycle was actually exercised (the suite
halted on the `world_state` failure before any new upstream commit landed).

---

## What Worked
- Full pull → doctor → CARLA + map verification → pytest → scenario suite
  loop, end to end, against real CARLA, including a genuine (not
  precondition-path) `mirror` PASS with two real simulators.
- `connectivity`, `sustained_stream`, `reconnect` all PASS same-machine; the
  map-mismatch guard and server-restart recovery both work as designed.
- The agent-to-agent mailbox is a real improvement over relaying text through
  the human — used it for several rounds this session without issue.
- Cross-machine connectivity over Tailscale works: `connectivity` PASSed
  cross-machine on the first real attempt, no networking setup needed beyond
  what was apparently already there.

## What Broke / What Needs Fixing
- **New, unresolved**: `world_state` topic-filtering leak on a cross-machine
  run — see "Cross-machine" above. This is the top priority for next session;
  it's a real server-side correctness bug (a subscribed-only client seeing a
  peer's/ego's topics is a privacy/bandwidth issue in real use, not just a
  test failure).
- Already fixed by the laptop side this session (for reference, not
  actionable again): the mirror/shadow-port collision, the `_format_summary`
  crash, the client-worker startup-ordering bug, and the non-hermetic pytest
  doctor test.
- Not yet investigated: whether `--auto-sync`'s retry-after-sync-only policy
  (vs. also retrying plain flakes) is the right call — laptop side asked for
  my take; deferred, since the auto-sync loop itself is currently off pending
  the operator's comfort with the no-auth situation.

---

## Request to Laptop Claude
- The `world_state` leak (see above) is the main thing worth chasing next —
  it reproduced on your machine, not mine, so you may be better positioned to
  add a regression test around `BroadcastThread._filter`'s cache keying.
- Auto-sync is OFF here for now (operator stepping away, coordinator has no
  auth). If you want to keep exercising it, that's your call on your side of
  the tailnet, but it won't be picked up from this machine until a session
  resumes here and turns it back on.
- Shadow CARLA on 2003 (with the map loaded) and the primary with ~6 vehicles
  of traffic were both left running after the coordinator/data server were
  stopped — available to reconnect to directly if useful, otherwise they'll
  likely get cleaned up next session.
- Full run/message history is on disk here (`.orchestration/runs/`,
  `.orchestration/messages.json`) if you need anything not summarized above.
