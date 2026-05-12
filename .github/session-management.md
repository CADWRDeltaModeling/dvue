# dvue Session Management

## Problem

`panel serve` creates a new Python object for every HTTP request.  Navigating
away and back, or restarting the browser, loses all UI state because the
application callable is re-executed in a fresh Bokeh Document.

---

## Design: Manager Registry + Cookie + diskcache Fallback

### Core Idea

Split the application into two lifetimes:

| Object | Lifetime | Notes |
|---|---|---|
| **Manager** (`param.Parameterized`) | Persistent, per user | Holds `time_range`, catalog, math refs, filter params — no Bokeh models |
| **DataUI** (Bokeh Document) | Per session (per browser tab open) | Creates fresh Bokeh models each time; wraps the manager |

A server-side dict (`_MANAGER_REGISTRY`) maps a stable user UUID to the
manager instance.  When a returning user opens a new Bokeh session, the
existing manager is retrieved from the registry and wrapped in a fresh
`DataUI`.  All param state is already correct — no deserialization needed.

### User Identity: `dvue_user_id` Cookie

- Value: `uuid4().hex` (32 hex chars)
- Lifetime: 365 days (persistent across browser restarts)
- Set by `_SessionAwareDocHandler.get()` on first visit

**Critical timing:** `_SessionAwareDocHandler` overrides `DocHandler.get()`
and injects the generated UUID into `self.request.cookies` (Tornado
`SimpleCookie`) *before* calling `super().get()`.  This makes the cookie
visible to `pn.state.cookies` inside the session factory on the very first
visit — before the browser has sent the cookie back.

```python
class _SessionAwareDocHandler(DocHandler):
    async def get(self, *args, **kwargs):
        user_id = self.get_cookie("dvue_user_id")
        if not user_id:
            user_id = uuid4().hex
            self.set_cookie("dvue_user_id", user_id, expires_days=365, path="/")
            self.request.cookies["dvue_user_id"] = user_id  # inject for same-request reads
        await super().get(*args, **kwargs)

per_app_patterns[0] = (r"/?", _SessionAwareDocHandler)
```

### Session Lifecycle

```
HTTP GET arrives
│
├─ _SessionAwareDocHandler.get()
│     Set / inject dvue_user_id cookie
│     Call super().get() → Bokeh creates new session → calls make_app()
│
└─ make_app()  (called per session by Panel/Bokeh)
      │
      ├─ user_id = pn.state.cookies.get("dvue_user_id")
      │
      ├─ user_id in _MANAGER_REGISTRY?
      │     YES → reuse existing manager (all params already set)
      │
      └─ NO  → create new manager
               user_id in diskcache?
                 YES → _restore_params(mgr, saved)  ← server-restart recovery
                 NO  → use defaults
               _MANAGER_REGISTRY[user_id] = entry

      Create DataUI(manager) → fresh Bokeh Document each time
      Register pn.state.onload(_on_load)

pn.state.onload fires (WebSocket opened)
      Replay plot_history: for each saved group call plot_cb sequentially
      Restore current_selection to display_table
      Wire param watchers → _save_state(user_id, ...) on every change
```

### Multi-Browser-Tab Behavior (same user)

Same `dvue_user_id` cookie → same manager instance.  Changing `time_range`
in tab A immediately affects tab B because both `DataUI` instances watch the
same `param` object.  This is intentional ("sync behavior").

Different users (different cookies) get independent manager instances.

### Two-Layer Persistence

| Layer | When active | What is stored |
|---|---|---|
| **Registry** (in-memory) | Server still running | `manager` instance + `plot_history` + `current_selection` |
| **diskcache** (disk) | Server restart recovery | Picklable params only: `time_range`, `current_selection`, `plot_history` |

diskcache advantages over hand-rolled JSON: built-in TTL, file-locking safe
for concurrent writes, already a Panel dependency (`pn.state.as_cached`).

**Limitation:** diskcache cannot store live Panel/HoloViews/Bokeh objects.
Only plain picklable Python values (dicts, lists, ISO date strings) are stored.

### Plot Tab Restore

Panel widgets (`pn.Tabs`, `pn.Row`, `hv.HoloViews` panes, etc.) maintain their
Python-level state (`.objects`, param values) **independently of any Bokeh
Document**.  When `template.servable()` is called for a new Bokeh session,
Panel creates fresh Bokeh models for the new document that mirror the current
Python state — including all plot tabs already present in `_display_panel`.

This means: for the registry hit path (server running), no replay logic is
needed.  The `DataUI` object is the same Python object and all its tab content
is automatically mirrored into the new Bokeh Document.

For the diskcache fallback (server restart), the `DataUI` is recreated fresh.
Only the last `current_selection` is saved and a single plot callback is
re-triggered.  Multiple open tabs are not restored after a server restart;
this is an acceptable limitation.

### Why `run_server.py` / `pn.serve(callable)`, Not `panel serve script.py`

`per_app_patterns[0]` must be patched before `BokehServer.__init__()`.
With `panel serve script.py`:
- `--setup` runs after the server starts → too late
- Script module-level code runs per session → too late

Only a programmatic launch (`python run_server.py` or `python script.py`)
executes the patch at the right moment.  `pn.serve()` must receive a
**callable** (`make_app`), not a file path, so the module-level manager
creation code runs only once.

### Known Limitations

1. `setup_url_sync` on the manager accumulates `param.watch` watchers across
   sessions (one per session for the same user).  Dead watchers (from ended
   sessions) fire harmlessly because they guard `if not pn.state.location:
   return`.  Net effect: redundant URL writes, not incorrect behavior.
2. `pn.Tabs(dynamic=True)` renders tab content lazily on the client.  Layer
   1 (registry) avoids this entirely by creating a fresh DataUI each session.
3. `num_procs=1` required: `_MANAGER_REGISTRY` is an in-process dict.
   Multi-process deployments must use an external store (Redis, shared memory)
   for the registry.  The diskcache fallback works with any `num_procs`.

---

## Files

| File | Role |
|---|---|
| `examples/ex_url_state_reset.py` | Phase 0 proof-of-concept (simple selection registry) |
| `examples/ex_tsdataui.py` | Phase 1 full demo (manager registry, plot_history) |
| `dvue/session_persistence.py` | *(planned)* Reusable module extracted from examples |
