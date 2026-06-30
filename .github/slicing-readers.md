# SlicingReader Architecture

`dvue/animator/reader.py` — how the reader hierarchy works, why each layer exists,
and which stack to use for a given scenario.

---

## 1. Core contract

Every reader in the hierarchy inherits from `SlicingReader` and obeys the same
external interface:

```
property  time_index  → pd.DatetimeIndex    # regular (freq is not None)
property  freq        → pd.DateOffset
property  vmin        → float               # global min across all geo_ids / time
property  vmax        → float               # global max

get_slice(timestamp)              → pd.Series(index=geo_ids, values=float)
get_slice_nearest(dt)             → pd.Series   # nearest-index convenience wrapper
get_slice_range(start, end)       → pd.DataFrame(index=timestamps, columns=geo_ids)
```

- `time_index` **must be regular** (`freq is not None`).  A `ValueError` is raised at
  construction if not.
- `get_slice` receives an **exact** timestamp from `time_index`.
- `get_slice_nearest` converts any `datetime`-like via
  `DatetimeIndex.get_indexer([ts], method="nearest")` — safe for UI slider values.
- `get_slice_range(start, end)` reads a contiguous block `[start, end)` indexed by
  integer position.  The default base-class implementation calls `get_slice` in a
  Python loop; subclasses that own bulk I/O (HDF5, Zarr) **must override this** for
  performance.

`vmin` / `vmax` are typically sampled from a small central window of the dataset at
construction so the colour scale is initialised without loading the full file.

---

## 2. Class hierarchy

```
SlicingReader   (ABC — dvue, reader.py)
│
├── InMemorySlicingReader
│       Wraps a pd.DataFrame already in RAM.
│       vmin/vmax: nanmin/nanmax of the whole array at __init__.
│
├── BufferedSlicingReader
│       Per-frame serving layer — keeps a rolling window of output frames in RAM.
│       Wraps any SlicingReader.  Adds:
│         • single-buffer synchronous refill (default)
│         • double-buffer async prefetch (prefetch=True)
│         • adaptive chunk sizing
│
├── TransformedSlicingReader          [legacy / backward-compat]
│       Loads the ENTIRE inner dataset into RAM on first access,
│       applies a bare callable transform once, then serves from RAM.
│
├── StreamingTransformedSlicingReader  [preferred for all production use]
│       Applies a TransformSpec transform per-chunk.
│       Zero full-file reads; startup is near-instant.
│
├── RawSequentialBuffer
│       Sits between a raw HDF5 reader and StreamingTransformedSlicingReader.
│       Caches bulk get_slice_range calls; pipelines HDF5 I/O with transform
│       computation by pre-reading the next raw window in a daemon thread.
│
└── DiffSlicingReader
        Computes A − B element-wise on a shared common time index.
        Wraps two readers of any concrete type.
```

---

## 3. Reader descriptions

### 3.1 `InMemorySlicingReader`

**Use when**: all data fits in RAM (test fixtures, pre-loaded arrays, small studies).

```python
reader = InMemorySlicingReader(df)   # df: DatetimeIndex rows × geo_id columns
```

`get_slice_range` is a single `DataFrame.iloc` slice — pure RAM, no I/O.

---

### 3.2 `BufferedSlicingReader`

**Use when**: wrapping any reader to serve per-frame `get_slice` calls efficiently
during animation playback.

The consumer (UI slider callback) calls `get_slice(ts)` once per animation frame.
Without buffering, each frame would trigger a fresh HDF5 read.  This reader keeps a
`chunk_size`-step window in RAM and only refills (via a single `get_slice_range` call)
when the cursor nears an edge.

#### Synchronous mode (`prefetch=False`, default)

```
frame N:    cursor inside buf             → RAM lookup, no I/O
frame N+k:  cursor within refill_margin  → get_slice_range([new_start, new_end]) synchronously
```

A single HDF5 read blocks the caller (IOLoop thread) for the duration of the I/O +
transform.  Acceptable for in-memory or fast data sources.

#### Prefetch mode (`prefetch=True`)

Adds a second buffer `_next_buf` pre-loaded by a daemon thread, so the next chunk
is ready before the cursor reaches the edge:

```
frame N:    cursor nears edge of buf  → _maybe_prefetch() spawns daemon thread
                                         (loads next chunk via get_slice_range)
frame N+k:  cursor reaches edge       → _try_promote() swaps next_buf → buf
                                         no I/O on calling thread; zero stall
random seek → _try_promote() fails   → synchronous _load_chunk(); _clear_next()
```

Thread safety: `_next_buf`, `_next_start`, `_next_end`, `_prefetching` are guarded
by `threading.Lock()`.  The consumer thread never writes these fields — only reads
under the lock.  The daemon thread only writes under the lock.

#### Adaptive chunk sizing

When `adaptive=True` (default, only active in prefetch mode), the buffer measures
playback frame rate from the last 20 `monotonic()` timestamps and adjusts
`chunk_size` so the buffer covers `target_buffer_seconds` (default 10 s) of real
playback:

```python
fps = (N_frames - 1) / elapsed_seconds
new_chunk_size = clamp(fps × target_buffer_seconds, min_chunk_size, max_chunk_size)
```

Chunk size only changes if the new value differs by > 20% — prevents rapid
oscillation.

**Key parameters**:

| Parameter | Default | Meaning |
|---|---|---|
| `chunk_size` | 200 | Steps per buffer fill |
| `refill_margin` | 0.15 | Refill when within 15% of edge |
| `prefetch` | False | Enable async double-buffer |
| `adaptive` | True | Auto-size chunks to playback rate |
| `min_chunk_size` | 50 | Adaptive lower bound |
| `max_chunk_size` | 2000 | Adaptive upper bound |
| `target_buffer_seconds` | 10.0 | Target wall-clock seconds of buffer |

---

### 3.3 `TransformedSlicingReader`  *(legacy)*

**Use when**: the transform is a bare callable and you do not need streaming
(e.g. small test data, custom one-off transforms, backward-compat code).

```python
reader = TransformedSlicingReader(inner, transform_fn=lambda df: df.resample("D").mean())
```

On first access to `time_index`, `vmin`, `vmax`, `get_slice`, or `get_slice_range`:
1. Reads the **entire** inner dataset via `inner.get_slice_range(0, n_raw)`.
2. Calls `transform_fn(raw_df)`.
3. Caches the result in RAM.

All subsequent calls are served from the cached DataFrame.

**Pitfall**: for a multi-year hourly file this loads the whole dataset at the first
slider movement, causing a multi-second freeze.  Use
`StreamingTransformedSlicingReader` for production.

---

### 3.4 `TransformSpec` + `StreamingTransformedSlicingReader`

**Use when**: applying a time-domain transform to a large HDF5 file without loading
it all at startup.  This is the preferred path for all DSM2 built-in transforms.

#### `TransformSpec`

A descriptor (not a bare callable) that carries the metadata needed to apply a
transform per-chunk without context loss at chunk boundaries:

```python
TransformSpec(
    transform_fn,           # callable(df) -> df
    kind,                   # "convolution" | "aggregate"
    get_overlap,            # callable(freq_nanos: int) -> int (steps on each side)
    output_freq=None,       # pandas offset string (aggregate only); None for convolution
)
```

| `kind` | Meaning | Output length |
|---|---|---|
| `"convolution"` | Rolling mean, Godin filter — bounded neighbourhood per output step | Same as input |
| `"aggregate"` | Resample / bin — contiguous range per output step | Shorter (coarser freq) |

`get_overlap` tells the reader how many **extra raw steps** to fetch on each side of
a requested window to avoid NaN at chunk boundaries.  For Godin at 1-hour data this
is 34 steps; at 15-minute data, 134 steps.

#### `StreamingTransformedSlicingReader`

Wraps any `SlicingReader` and a `TransformSpec`.  At construction:
- **`time_index`**: computed from inner's metadata — pure datetime arithmetic, no I/O.
- **`vmin`/`vmax`**: sampled from `sample_steps` (default 200) output frames from the
  centre of the file — one small HDF5 read.

Per `get_slice_range(start_out, end_out)` call:

*Convolution* (rolling, Godin):
```
raw_start = max(0, start_out - overlap)
raw_end   = min(N, end_out   + overlap)
left_pad  = start_out - raw_start      (actual overlap added on left)
raw_df    = inner.get_slice_range(raw_start, raw_end)
out_df    = transform_fn(raw_df)
return out_df.iloc[left_pad : left_pad + (end_out - start_out)]
```

*Aggregate* (daily resample):
```
raw_start = searchsorted(out_time[start_out])
raw_end   = searchsorted(out_time[end_out - 1] + out_freq)
raw_df    = inner.get_slice_range(raw_start, raw_end)
return transform_fn(raw_df).reindex(out_time[start_out:end_out])
```

**Key consequence**: every 200-frame buffer refill causes one HDF5 read of
`200 + 2 × overlap` raw steps and one transform call.  For a 100-year hourly file
this reads < 0.03% of the data per chunk.

#### DSM2 built-in transform factories (in `dsm2ui.animate`)

| Factory | Kind | `get_overlap` | Effect |
|---|---|---|---|
| `make_resample_transform(freq, agg)` | aggregate | 0 | `df.resample(freq).mean()` |
| `make_moving_average_transform(window)` | convolution | `window // 2` | centred rolling mean |
| `make_godin_transform()` | convolution | `(30h + 24h) / 2 / freq_nanos` | Godin tidal filter via `vtools3.cosine_lanczos` |

---

### 3.5 `RawSequentialBuffer`

**Use when**: a `StreamingTransformedSlicingReader` + HDF5 reader stack is bottlenecked
on HDF5 I/O latency because transform computation and I/O run **sequentially** on the
same thread.

`RawSequentialBuffer` (RSB) sits *below* STSR and *above* the raw HDF5 reader.  Its
job is to pre-read the **next raw window** while the current window is being
transformed, so I/O and transform overlap in wall-clock time.

#### Full production stack

```
BufferedSlicingReader(prefetch=True)         ← per-frame serving to UI
  └── StreamingTransformedSlicingReader       ← transform per chunk (Godin + daily)
      └── RawSequentialBuffer(prefetch_enabled=True)
          └── HydroH5FlowReader              ← h5py bulk reads
```

#### Cache structure

RSB maintains two DataFrames:

| Name | Written by | Meaning |
|---|---|---|
| `_cache` / `_cache_start` / `_cache_end` | consumer thread | Current serving window |
| `_next_cache` / `_next_start` / `_next_end` | daemon thread | Prefetched next window |

Both are guarded by `threading.Lock()`.

#### `get_slice_range(start_idx, end_idx)` logic

```
1. Cache hit: [start, end) ⊆ [_cache_start, _cache_end)
      → return _cache.iloc[s:e]
        _maybe_prefetch() to keep next window loading

2. Next-cache promotion: [start, end) ⊆ [_next_start, _next_end)
      → swap _next_cache → _cache (under lock)
        return slice from new _cache
        _maybe_prefetch() to load the window after that

3. Cache miss:
      → load EXACTLY [start_idx, end_idx] from inner (no synchronous lookahead)
        _maybe_prefetch() to start async prefetch of following window
        return slice
```

**Critical design choice**: on a cache miss, RSB loads **only the requested range**
synchronously.  Loading `lookahead_factor ×` more synchronously would add proportional
overhead on cache-miss paths (e.g. cold start, random seek) — the original regression
was +161.9% because the miss path loaded `5 × request_size` synchronously.

#### `_maybe_prefetch` — async prefetch window

```
prefetch_start = max(0, _cache_end - back_safety)
prefetch_size  = max(min_chunk_size, request_size × lookahead_factor)
prefetch_end   = min(N, prefetch_start + prefetch_size)
```

`back_safety` (default 300) ensures the prefetch window starts a few steps *before*
`_cache_end`.  This is needed because STSR's next `get_slice_range` call overlaps with
the current cache by `2 × overlap` steps (e.g. 68 steps for Godin bilateral), so the
next sequential request starts before `_cache_end`.  Without `back_safety` the next
request would miss the prefetch window and fall through to a synchronous HDF5 read.

#### `prefetch_enabled` flag

When `BufferedSlicingReader` is in **synchronous mode** (`prefetch=False`), it makes
multiple sequential `get_slice_range` calls on the consumer thread in rapid
succession.  If RSB had `prefetch_enabled=True`, it would launch a daemon thread after
each of these calls; the daemon's large async I/O (`4 × request_size`) would
**compete** with the consumer's next synchronous chunk load, adding I/O contention
overhead (~+34–73%).

Solution: pass `prefetch_enabled=prefetch` to RSB from the test/factory helpers.
In production (always `prefetch=True`), RSB always has `prefetch_enabled=True`.

**Key parameters**:

| Parameter | Default | Meaning |
|---|---|---|
| `lookahead_factor` | 4 | Async prefetch covers `4 × request_size` steps |
| `min_chunk_size` | 200 | Lower bound on prefetch size |
| `back_safety` | 300 | Steps before `cache_end` to start prefetch (covers bilateral filter overlap) |
| `prefetch_enabled` | True | Set False in sync-mode test stacks to avoid I/O contention |

#### Measured performance (HYDRO + QUAL DSM2 historical h5, 1-hour data, Godin + daily)

| Mode | Stack | Overhead vs no-RSB |
|---|---|---|
| sync forward sweep | `prefetch_enabled=False` | −0.2% (negligible) |
| sync random seeks | `prefetch_enabled=False` | +0.3% (negligible) |
| prefetch forward sweep | `prefetch_enabled=True` | HYDRO −11% worst-frame; QUAL −99.5% worst-frame (620 ms → 3 ms) |

QUAL benefits most because the Godin+daily transform is slow relative to I/O — the
4 × lookahead means the transform rarely has to wait for the HDF5 read.

---

### 3.6 `DiffSlicingReader`

**Use when**: displaying the element-wise difference between two studies
(A − B comparison mode in `MultiGeoAnimatorManager`).

```python
diff = DiffSlicingReader(reader_a, reader_b)
buffered = BufferedSlicingReader(diff, prefetch=True)
```

#### Common time index construction

```
start = max(reader_a.time_index[0], reader_b.time_index[0])
end   = min(reader_a.time_index[-1], reader_b.time_index[-1])
freq  = max(freq_a, freq_b)    # coarser of the two, by nanos
idx   = pd.date_range(start, end, freq=freq)
```

Raises `ValueError` if the two studies have no time overlap.

#### Per-frame computation

For each timestamp:
1. `reader_a.get_slice_nearest(ts)`
2. `reader_b.get_slice_nearest(ts)`
3. Return `a - b` (NaN-safe; missing geo_ids produce NaN).

`get_slice_range` iterates over the common time index in a Python loop — acceptable
because `DiffSlicingReader` is always wrapped by `BufferedSlicingReader`.

#### `vmin` / `vmax`

Estimated from the first 20 diff steps; made **symmetric around zero**:
```python
absmax = max(|vmin_sample|, |vmax_sample|)
vmin = -absmax;  vmax = +absmax
```

Ensures diverging colormaps (`coolwarm`, `RdBu_r`) are centred at zero by default.

Apply transforms to `reader_a` and `reader_b` individually **before** constructing
`DiffSlicingReader`; do not wrap the diff reader with `TransformedSlicingReader`.

---

## 4. Recommended stacks

### Raw / in-memory data (tests, pre-loaded)

```
BufferedSlicingReader
  └── InMemorySlicingReader(df)
```

### HDF5 file, no transform (raw playback)

```
BufferedSlicingReader(prefetch=True)
  └── HydroH5FlowReader(h5_path)          # or any HDF5 SlicingReader
```

### HDF5 file + fast transform (daily resample — no overlap / short overlap)

```
BufferedSlicingReader(prefetch=True)
  └── StreamingTransformedSlicingReader(spec)
      └── HydroH5FlowReader
```

### HDF5 file + slow / large-overlap transform (Godin, rolling 14D)

```
BufferedSlicingReader(prefetch=True)
  └── StreamingTransformedSlicingReader(spec)
      └── RawSequentialBuffer(prefetch_enabled=True)
          └── HydroH5FlowReader
```

RSB pipelines the HDF5 read for the next transform window with the current transform
computation.  Without RSB, STSR's `get_slice_range` blocks waiting for the HDF5 read
every chunk refill — adding 1–2 s stalls every few hundred frames.

### Two-study comparison (diff mode)

```
BufferedSlicingReader(prefetch=True)
  └── DiffSlicingReader
      ├── [stack A]    (any of the above stacks)
      └── [stack B]    (any of the above stacks)
```

---

## 5. Threading model

All async work uses **daemon threads** so they are silently killed when the process
exits.  No explicit thread lifecycle management is needed.

```
Consumer thread  (IOLoop / Panel watcher)
│
│   get_slice(ts) → BufferedSlicingReader
│       if hit: RAM slice
│       if near edge: _maybe_prefetch() ──────────────────────► Daemon A
│       if miss: _load_chunk()  (sync; blocks consumer)         (loads BSR._next_buf)
│                   get_slice_range()
│                     → StreamingTransformedSlicingReader
│                         → RawSequentialBuffer.get_slice_range()
│                             if hit: RAM slice
│                             if miss: inner.get_slice_range() ──► Daemon B
│                                      (loads RSB._next_cache)
│
│   _try_promote(): BSR swaps _next_buf → _buf  (under lock)
│   RSB.get_slice_range() sees next-cache hit:   (under lock)
│       promotes _next_cache → _cache
```

**Daemon A** (BSR prefetch worker): runs BSR._prefetch_worker; calls
`get_slice_range` on the transform stack (STSR → RSB → HDF5).

**Daemon B** (RSB prefetch worker): runs RSB._prefetch_worker; calls
`inner.get_slice_range` on the raw HDF5 reader only.

In practice only one daemon is active at a time per reader stack: BSR's daemon
triggers RSB's miss path synchronously (inside `_prefetch_worker`) and RSB then
launches its own daemon for the level below that.

**Lock discipline**:
- `BSR._lock` guards `_next_buf`, `_next_start`, `_next_end`, `_prefetching`.
- `RSB._lock` guards `_next_cache`, `_next_start`, `_next_end`, `_prefetching`.
- Consumer thread never holds either lock across I/O.
- Daemon threads hold lock only to write results and clear `_prefetching`.

---

## 6. Invariants and edge-case handling

| Situation | Behaviour |
|---|---|
| Cursor at file start/end | `_needs_refill` never triggers on a clamped boundary; no repeated re-reads |
| Random seek (DatetimePicker jump) | BSR: `_try_promote` fails → synchronous `_load_chunk` + `_clear_next`. RSB: cache miss → exact synchronous load + new prefetch |
| Transforms with `warmup_steps` | `TransformedSlicingReader` drops leading NaN rows from cached output; `StreamingTransformedSlicingReader` relies on `get_overlap` from `TransformSpec` |
| Irregular output from transform | `STSR.get_slice_range` raises `ValueError` if `transform_fn` returns an irregular DatetimeIndex |
| Empty HDF5 window | `get_slice_range` returns empty DataFrame; callers receive `pd.Series(dtype=float)` |
| BSR refill margin at file boundary | Only near non-clamped edges triggers refill — prevents infinite re-read at the first/last chunk |
| `DiffSlicingReader` with non-overlapping studies | Raises `ValueError` with human-readable message including both study time ranges |

---

## 7. Adding a new raw reader (HDF5, NetCDF, Zarr, …)

```python
class MyH5Reader(SlicingReader):
    def __init__(self, filepath, ...):
        import h5py
        self._h5 = h5py.File(filepath, "r")
        self._ds = self._h5["/some/dataset"]      # shape: (n_time, n_geo)
        start = pd.Timestamp(attrs["start_time"].decode())
        freq  = pd.to_timedelta(attrs["interval"].decode())
        idx   = pd.date_range(start=start, periods=self._ds.shape[0], freq=freq)
        # Sample vmin/vmax from centre of file (no full read):
        mid = idx.shape[0] // 2
        sample = self._ds[mid - 100: mid + 100, :]
        self._vmin = float(np.nanmin(sample))
        self._vmax = float(np.nanmax(sample))
        self._geo_ids = list(range(self._ds.shape[1]))   # or from metadata
        super().__init__(idx)

    @property
    def vmin(self): return self._vmin
    @property
    def vmax(self): return self._vmax

    def get_slice(self, timestamp):
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        return pd.Series(self._ds[i, :], index=self._geo_ids, dtype=float)

    def get_slice_range(self, start_idx, end_idx):
        arr = self._ds[start_idx:end_idx, :]          # ONE contiguous h5py read
        ts  = self._time_index[start_idx:end_idx]
        return pd.DataFrame(arr, index=ts, columns=self._geo_ids, dtype=float)

    def close(self):
        self._h5.close()
```

Then wrap with the appropriate stack from §4.
