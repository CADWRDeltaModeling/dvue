"""Quick test: can pn.state.location.sync handle our param types?"""
import param
import panel as pn
import datetime

print("param", param.__version__)
print("panel", pn.__version__)

class MockManager(param.Parameterized):
    time_range = param.CalendarDateRange(default=None)
    fill_gap = param.Integer(default=0)
    do_tidal_filter = param.Boolean(default=False)
    resample_period = param.String(default="")
    resample_agg = param.Selector(default="mean", objects=["mean","max","min","sum","std"])
    scale_factor = param.Number(default=1.0)
    sensible_percentile_range = param.Range(default=(0.01, 0.99), bounds=(0, 1), step=0.01)
    regular_curve_connection = param.Selector(
        objects=["linear", "steps-pre", "steps-post", "steps-mid"], default="steps-post")

m = MockManager()
m.time_range = (datetime.date(2020, 1, 1), datetime.date(2021, 1, 1))

# Simulate what location.sync does internally: serialize each param value
for name in ["time_range", "fill_gap", "do_tidal_filter", "resample_period",
             "resample_agg", "scale_factor", "sensible_percentile_range",
             "regular_curve_connection"]:
    val = getattr(m, name)
    p = m.param[name]
    # Panel location.sync uses param's own serialization
    try:
        serialized = p.serialize(val)
        print(f"  {name}: {val!r} -> serialize -> {serialized!r}")
    except Exception as e:
        print(f"  {name}: {val!r} -> serialize FAILED: {e}")
    # Also test str round-trip
    try:
        s = str(val)
        print(f"  {name}: str() -> {s!r}")
    except Exception as e:
        print(f"  {name}: str() FAILED: {e}")

print("\nDone.")
