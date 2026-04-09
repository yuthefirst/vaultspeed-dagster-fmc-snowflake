# Plan: Add `--load-type` Filter to `kris.py`

## File to Modify
- `kris.py`

## Problem

When `kris.py` runs, it builds aggregate strings (`jbsd`, `jbsdi`, `snsrl`, `schl`) from only the FMC files it processes. These strings are then used to **overwrite entire lines** in the generated files (`sensors/__init__.py`, `schedules/__init__.py`, `__init__.py`). If we filter to only INCR, those strings contain only INCR items and the overwrite erases all INIT definitions — and vice versa.

## Changes (3 parts)

### 1. Add argparse CLI argument (top of file, before `if __name__`)

Add `import argparse` and a parser with `--load-type` accepting `INIT`, `INCR`, or `ALL` (default `ALL`).

### 2. Filter FMC files (line ~101, inside the `for fmcFile in fmcFiles` loop)

After loading `fmcInfo`, skip files whose `load_type` doesn't match the filter:
```python
if selected_load_type != 'ALL' and fmcInfo['load_type'] != selected_load_type:
    print(f"Skipping {fmcInfo['load_type']} FMC from {fmcFile} (filtered by --load-type {selected_load_type})")
    continue
```

### 3. Make aggregate string construction merge-aware (lines 226–235, 238, 259–272, 308–341, 364–381)

Add two helper functions before `class defAsset`:

- `parse_items_from_line(line)` — extracts comma-separated identifiers from lines like `from ..jobs import a,b,c` or `all_jobs=[a,b,c]` or `@sensor(jobs=[a,b,c])` or `    jobs_to_monitor = [a,b,c]` or `    exclst = [a,b,c]`
- `merge_items(existing, new)` — returns ordered, deduplicated union

Then change how `jbsd`, `jbsdi`, `snsrl`, and `schl` are used:

**Jobs aggregate (`jbsd`/`jbsdi`)** — Before writing to `sensors/__init__.py` (line 259), parse the existing `from ..jobs import` line to get previously-defined items, merge with current `jbs`/`jbsi`, rebuild the string. Same for `@sensor(jobs=[...])` (line 262), `jobs_to_monitor` (line 265), `bvjb` (line 268), and `exclst` (line 271).

**Sensors aggregate (`snsrl`)** — Before building `snsrl` (line 238), parse existing sensor function names from `sensors/__init__.py` using regex on `^def (\w+)\(`. Seed `snsrl` with these names so existing sensors are preserved.

**Schedules aggregate (`schl`)** — The schedule section (lines 308–341) is already additive for individual schedule definitions. The only issue is the import line; apply the same parse-and-merge approach.

**Main `__init__.py`** (lines 364–381) — For each of the 6 overwrite patterns (`from .jobs import`, `all_jobs=`, `from .schedules import`, `all_schedules=`, `from .sensors import`, `all_sensors=`), parse the existing line first, merge with current items, then write the merged result. The asset import line (line 357) and asset variable/Definitions wiring (lines 382–396) are already additive — no changes needed.

### Summary of where aggregate overwrites happen and the fix

| Line | Pattern | Fix |
|------|---------|-----|
| 259 | `from ..jobs import {jbsd}` in sensors | Parse existing, merge with `jbs` |
| 262 | `@sensor(jobs=[{jbsdi}])` in sensors | Parse existing, merge with `jbsi` |
| 265 | `jobs_to_monitor = [{jbsdi}]` in sensors | Parse existing, merge with `jbsi` |
| 271 | `exclst = [{skp}]` in sensors | Parse existing, merge with `skp` |
| 364 | `from .jobs import {jbsd}` in main init | Parse existing, merge with `jbs` |
| 368 | `all_jobs=[{jbsd}]` in main init | Parse existing, merge with `jbs` |
| 371 | `from .schedules import {schl}` in main init | Parse existing, merge with `schs` |
| 374 | `all_schedules=[{schl}]` in main init | Parse existing, merge with `schs` |
| 377 | `from .sensors import {snsrl}` in main init | Parse existing, merge with sensors |
| 380 | `all_sensors=[{snsrl}]` in main init | Parse existing, merge with sensors |

## Verification

After implementation, verify with:
1. Confirm `python kris.py --load-type ALL` produces identical output to the current `python kris.py` (no behavioral change for default)
2. Confirm `python kris.py --load-type INCR` followed by `python kris.py --load-type INIT` produces the same final state as `--load-type ALL`
3. Confirm running the same `--load-type` twice is idempotent (no duplicates)
