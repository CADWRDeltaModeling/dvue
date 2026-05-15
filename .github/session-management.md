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

---

### VanillaTemplate + pn.state.onload — The Correct Serving Pattern

#### Use VanillaTemplate, not FastListTemplate

`FastListTemplate` wraps each `main` item in a Bootstrap `<li>` with fixed height,
which collapses the `GridStack` layout so the table and display panel disappear.
`VanillaTemplate` renders `main` items full-width without that wrapping — always use
it as the outer served template when embedding a `DataUI`.

#### Pre-import heavy modules before `pn.serve()`

`cdecuimgr.py` (and similar manager modules) call `hv.extension("bokeh")` at module
level.  If the module is first imported inside `make_app()` (a per-session callable),
that module-level call fires inside a live Bokeh session and can reset
`pn.state.curdoc`, orphaning existing Bokeh models.

Import the manager class once, before `pn.serve()`:

```python
from myapp.mymanager import MyManager  # noqa: F401  — side-effect: hv.extension()
pn.serve({"route": make_app}, ...)
```

#### `template.servable()` must be called synchronously in `make_app()`

```python
def make_app():
    ...
    template = pn.template.VanillaTemplate(...)
    pn.state.onload(_load_app)
    template.servable()   # ← REQUIRED here, NOT only inside pn.state.onload
```

Without `template.servable()` in the synchronous body of `make_app()`, the Bokeh
document is never registered and the browser receives an empty page.

#### Critical: VanillaTemplate DOM Placeholder Rule

`VanillaTemplate`'s Jinja HTML embeds Bokeh roots for items that exist **when
`.servable()` is called**.  Items appended to `template.header` or `template.modal`
inside `pn.state.onload` (which fires *after* `.servable()`) have no `<div>`
placeholder in the served HTML and will never render.

The vanilla.html Jinja template loops over `doc.roots` at page-load time:

```html
<!-- header slot — only roots that exist at .servable() time are embedded -->
{% for doc in docs %}{% for root in doc.roots %}
{% if "header" in root.tags %}{{ embed(root) }}{% endif %}
{% endfor %}{% endfor %}

<!-- modal slot — same rule -->
{% for doc in docs %}{% for root in doc.roots %}
{% if "modal" in root.tags %}{{ embed(root) }}{% endif %}
{% endfor %}{% endfor %}
```

**Fix — pre-render container objects before `.servable()`, populate inside `_load_app`:**

```python
def make_app():
    header_row = pn.Row(sizing_mode="fixed")           # created before .servable()
    modal_pane = pn.Column(sizing_mode="stretch_width") # created before .servable()

    template = pn.template.VanillaTemplate(
        ...,
        header=[header_row],          # embedded in Jinja HTML at page-load time
    )
    template.modal.append(modal_pane) # also before .servable()

    def _load_app():
        ...
        # Swap contents of the pre-rendered containers.
        # NEVER call template.modal.clear() — it removes the pre-rendered root.
        modal_pane.objects = [ui.get_about_text()]

        about_btn = pn.widgets.Button(name="About App", button_type="primary", icon="info-circle")
        def _about_click(event):
            modal_pane.objects = [ui.get_about_text()]
            template.open_modal()
        about_btn.on_click(_about_click)

        disclaimer = mgr.get_sidebar_disclaimer()
        if disclaimer:
            dis_btn = pn.widgets.Button(name="Disclaimer", button_type="light", icon="alert-circle")
            def _dis_click(event):
                modal_pane.objects = [disclaimer]
                template.open_modal()
            dis_btn.on_click(_dis_click)
            header_row.append(dis_btn)

        header_row.append(about_btn)

    pn.state.onload(_load_app)
    template.servable()   # ← registers the pre-rendered header_row and modal_pane roots
```

> **Never call `template.modal.clear()` after `.servable()`.**  It destroys the
> pre-rendered `modal_pane` root permanently; all subsequent `template.open_modal()`
> calls open an empty dialog.

#### Full make_app() Reference Implementation

```python
def make_app():
    user_id  = session_mgr.current_user_id
    reg_key  = session_mgr.make_reg_key(user_id, "myapp")
    entry    = session_mgr.get_entry(reg_key)

    main_panel    = pn.Column(pn.indicators.LoadingSpinner(value=True, color="primary", size=50),
                              sizing_mode="stretch_both")
    sidebar_panel = pn.Column(pn.indicators.LoadingSpinner(value=True, color="primary", size=50))
    header_row    = pn.Row(sizing_mode="fixed")
    modal_pane    = pn.Column(sizing_mode="stretch_width")

    if entry:
        # Registry hit — reuse the already-built template and DataUI.
        template   = entry["template"]
        ui         = entry["ui"]
        stored_main = entry.get("main_panel")
        if stored_main is not None:
            stored_main.loading = True
        def _reattach():
            try:
                ui.setup_location_sync()
                ui.setup_url_sync()
            finally:
                if stored_main is not None:
                    stored_main.loading = False
        pn.state.onload(_reattach)
        template.servable()
        return

    template = pn.template.VanillaTemplate(
        title="My App",
        sidebar=[sidebar_panel],
        main=[main_panel],
        header=[header_row],       # pre-rendered header container
    )
    template.modal.append(modal_pane)  # pre-rendered modal container

    def _load_app():
        try:
            mgr = build_manager()
            ui  = DataUI(mgr, crs=..., station_id_column="ID")
            ui_template = ui.create_view(title="My App")

            # Transplant sidebar/main into the outer VanillaTemplate.
            sidebar_panel.objects = list(ui_template.sidebar)
            main_panel.objects    = list(ui_template.main)
            ui_template.sidebar.clear()
            ui_template.main.clear()
            ui_template.modal.clear()  # safe: clears inner FastListTemplate only

            # Populate the pre-rendered header/modal containers.
            about_text = ui.get_about_text()
            modal_pane.objects = [about_text]

            about_btn = pn.widgets.Button(name="About App", button_type="primary", icon="info-circle")
            def _about_click(event):
                modal_pane.objects = [about_text]
                template.open_modal()
            about_btn.on_click(_about_click)

            disclaimer = mgr.get_sidebar_disclaimer()
            if disclaimer:
                dis_btn = pn.widgets.Button(name="Disclaimer", button_type="light", icon="alert-circle")
                def _dis_click(event):
                    modal_pane.objects = [disclaimer]
                    template.open_modal()
                dis_btn.on_click(_dis_click)
                header_row.append(dis_btn)

            header_row.append(about_btn)

            session_mgr.set_entry(reg_key, {
                "template": template, "ui": ui,
                "main_panel": main_panel,
                "header_row": header_row, "modal_pane": modal_pane,
            })
        except Exception:
            import traceback
            logger.error("_load_app failed:\n%s", traceback.format_exc())
            main_panel.objects = [pn.pane.Markdown("## Error\n\nCheck server logs.")]

    pn.state.onload(_load_app)
    template.servable()
```



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
