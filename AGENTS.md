# dvue — Agent Instructions

## Scope

Use this file when working in the `dvue/` workspace root.

## Purpose

`dvue` is a reusable UI/data-catalog framework for Panel + HoloViews + GeoViews apps. It should stay domain-agnostic.

## Fast Start For Agents

1. Install for development:
   - `pip install -e ".[dev]"`
2. Run tests:
   - `pytest`
3. Run examples:
   - `panel serve examples/ex_basic_tsdataui.py --show`
   - `panel serve examples/ex_tsdataui.py --show`

## Key Design Rules

- Keep `dvue` generic. Do not hard-code datastore-specific naming or repo semantics.
- Extend with subclassing/composition from app code (`DataUIManager`, `TimeSeriesDataUIManager`, action classes, catalog builders).
- Preserve metadata-driven behavior in catalog objects; avoid adding hidden coupling between UI and specific datasets.

## Primary Files To Read First

- `dvue/dataui.py` (base manager/provider patterns)
- `dvue/tsdataui.py` (time-series UI manager behavior)
- `dvue/catalog.py` (`DataReference`, `DataCatalog`, builders, math references)
- `dvue/actions.py` (default actions + extension points)
- `README.md` and `docs/Architecture.md` (usage and architecture)

## Conventions And Pitfalls

- Keep API surface backward-compatible where practical; downstream apps subclass these components.
- Ensure Panel/HoloViews extensions are initialized consistently when changing startup behavior.
- When adding catalog attributes used for filtering/grouping, document them and keep table/map selection flow intact.
- If changing math-reference behavior, update examples and docs linked from `README-mathref.md`.

## Downstream Integration Notes

- `dms_datastore_ui` uses `TimeSeriesDataUIManager` subclassing and custom actions.
- For integration-specific behavior, update downstream project code rather than adding domain logic in `dvue`.
- See `../dms_datastore_ui/AGENTS.md` for datastore dashboard conventions.
