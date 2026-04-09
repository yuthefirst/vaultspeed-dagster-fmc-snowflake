# Separating INIT vs INCR Workflow Generation

## Problem Statement

Today, `kris.py` processes all FMC JSON files in a single pass, generating both INIT and INCR assets, jobs, schedules, and sensors together into one unified Dagster project. While the generated outputs are already separated by naming convention (`init_*.py` vs `incr_*.py`), the generation process itself treats them identically. This creates challenges:

- **Deployments are coupled** — a VaultSpeed re-export of INCR mappings forces regeneration of INIT artifacts (and vice versa)
- **No independent lifecycle** — INIT loads are ad-hoc and rarely change, while INCR mappings evolve with source system changes
- **Risk of accidental overwrites** — regenerating for one load type can clobber the other if not carefully managed
- **Build complexity** — a single monolithic generation step makes it harder to reason about what changed and why

---

## Current State: How INIT and INCR Differ

| Dimension | INIT | INCR |
|-----------|------|------|
| **Purpose** | Full historical load / bootstrap | Daily CDC-based incremental load |
| **Frequency** | One-time or rare | Scheduled (cron, typically 3 AM EST) |
| **Load date** | Fixed historical date (`2000-01-01`) or `start_date` from FMC_info | `datetime.now()` |
| **Source data** | Full snapshot tables (`*_INI` schema) | CDC views (`VW_*_CDC`) |
| **Schedules** | None (manual trigger) | Cron-based (`ScheduleDefinition`) |
| **Sensors** | Failure sensors only | Failure sensors + `incr_bv_generator` |
| **Procedure suffix** | `*_INIT` | `*_INCR` |
| **Typical use** | Dev/test resets, new source onboarding | Production daily operations |

Despite these differences, `kris.py` uses the same code path for both. The `load_type` field from `FMC_info_*.json` drives conditional logic for schedules and load dates, but generation is otherwise unified.

---

## Goal

Separate the **generation process** so that INIT and INCR workflows can be generated independently, while keeping all generated artifacts in a **single Dagster code location** (`DataWranglers/`).

This means:
- `kris.py` can be invoked to generate only INIT artifacts, only INCR artifacts, or both
- All output still lands in `DataWranglers/` — one `Definitions` object, one deployment
- The aggregated wiring files (`jobs/__init__.py`, `sensors/__init__.py`, `schedules/__init__.py`, `__init__.py`) must merge artifacts from separate generation runs without clobbering

---

## Files to Modify

| File | Change |
|------|--------|
| `kris.py` | Add `--load-type` CLI arg, FMC filtering, merge-aware aggregate writes |
| `.github/workflows/conv_FMC.yml` | Add `LoadType` workflow dispatch input, pass to Docker container |

---

## The Core Problem: Aggregate String Overwrites

`kris.py` builds aggregate comma-separated strings (`jbsd`, `jbsdi`, `snsrl`, `schl`) from only the FMC files processed in the current run. These strings are then used to **overwrite entire lines** in generated files. If we filter to only INCR, the strings contain only INCR items and the overwrite erases all INIT definitions.

### Lines That Overwrite (in `kris.py`)

| Line | File Being Written | Pattern | Risk |
|------|--------------------|---------|------|
| 259 | `sensors/__init__.py` | `from ..jobs import {jbsd}` | Loses other load type's jobs |
| 262 | `sensors/__init__.py` | `@sensor(jobs=[{jbsdi}])` | Loses INCR jobs from prior run |
| 265 | `sensors/__init__.py` | `jobs_to_monitor = [{jbsdi}]` | Same |
| 271 | `sensors/__init__.py` | `exclst = [{skp}]` | Loses exclusions |
| 364 | `__init__.py` | `from .jobs import {jbsd}` | Loses other load type's jobs |
| 368 | `__init__.py` | `all_jobs=[{jbsd}]` | Same |
| 371 | `__init__.py` | `from .schedules import {schl}` | Loses schedules |
| 374 | `__init__.py` | `all_schedules=[{schl}]` | Same |
| 377 | `__init__.py` | `from .sensors import {snsrl}` | Loses sensors |
| 380 | `__init__.py` | `all_sensors=[{snsrl}]` | Same |

### Lines That Are Already Additive (Safe)

| File | Pattern | Why It's Safe |
|------|---------|---------------|
| `assets/{group}.py` | One file per group | INCR run doesn't touch `init_*.py` files |
| `jobs/__init__.py` | Reads file, checks if `{group}_grp` exists, appends if missing | Already merge-safe |
| `sensors/__init__.py` | Failure sensor definitions (lines 286–304) | Checks if sensor exists, appends if missing |
| `schedules/__init__.py` | Schedule definitions (lines 330–341) | Checks if schedule exists, appends if missing |
| `__init__.py` | Asset import line (line 357), asset variable (line 386), Definitions assets list (line 387–393) | Already checks and appends |

---

## Implementation Plan

### Change 1: Add `--load-type` CLI Argument to `kris.py`

Add `import argparse` at the top. Before `if __name__ == '__main__':`, add:

```python
parser = argparse.ArgumentParser(description='VaultSpeed FMC to Dagster converter')
parser.add_argument('--load-type', choices=['INIT', 'INCR', 'ALL'], default='ALL',
                    help='Filter by load type: INIT, INCR, or ALL (default)')
args = parser.parse_args()
selected_load_type = args.load_type
```

### Change 2: Filter FMC Files

After loading `fmcInfo` (line 100), add:

```python
if selected_load_type != 'ALL' and fmcInfo['load_type'] != selected_load_type:
    print(f"Skipping {fmcInfo['load_type']} FMC from {fmcFile} (filtered by --load-type {selected_load_type})")
    continue
```

This is the only filtering needed — all downstream loops iterate `asstfls`, which will only contain groups matching the filter.

### Change 3: Add Merge Helper Functions

Add two utility functions before `class defAsset`:

```python
def parse_items_from_line(line):
    """Extract comma-separated identifiers from import/variable/decorator lines."""
    # Handles: 'from ..jobs import a,b,c'
    #          'all_jobs=[a,b,c]'
    #          '@sensor(jobs=[a,b,c])'
    #          '    jobs_to_monitor = [a,b,c]'
    #          '    exclst = [a,b,c]'
    ...

def merge_items(existing, new):
    """Return ordered, deduplicated union of two lists. Existing items keep position."""
    seen = set()
    merged = []
    for item in existing:
        if item not in seen:
            seen.add(item)
            merged.append(item)
    for item in new:
        if item not in seen:
            seen.add(item)
            merged.append(item)
    return merged
```

### Change 4: Make Aggregate Writes Merge-Aware

At each of the 10 overwrite points listed above, instead of writing the current run's string directly:

1. Parse the existing line from the file to extract previously-defined items
2. Merge with the current run's items using `merge_items()`
3. Write the merged result

**Example** — sensors `from ..jobs import` line (currently line 259):

```python
# Before (overwrites):
if 'from ..jobs import' in rws[line] and jbsd not in rws[line]:
    initf.write(f'from ..jobs import {jbsd}\n')

# After (merges):
if 'from ..jobs import' in rws[line]:
    existing = parse_items_from_line(rws[line])
    new = [j.strip() for j in jbsd.split(',') if j.strip()]
    merged = merge_items(existing, new)
    initf.write(f'from ..jobs import {",".join(merged)}\n')
```

Same pattern applies to all 10 overwrite points.

### Change 5: Preserve Sensor Names Across Runs

Currently `snsrl` starts as `"incr_bv_generator,"` (line 238) and accumulates only current-run sensors. When running `--load-type INIT`, `snsrl` would not include any INCR sensors.

**Fix**: Before building `snsrl`, parse existing sensor function names from `sensors/__init__.py`:

```python
import re
with open(f'/dagster/{prjnm}/sensors/__init__.py', 'r') as f:
    existing_sensor_content = f.read()
existing_sensor_names = re.findall(r'^def (\w+)\(', existing_sensor_content, re.MULTILINE)
snsrl = ','.join(existing_sensor_names) + ',' if existing_sensor_names else 'incr_bv_generator,'
```

### Change 6: Add `LoadType` Input to GitHub Actions Workflow

In `.github/workflows/conv_FMC.yml`, add a new workflow dispatch input:

```yaml
LoadType:
  description: Load Type Filter (INIT, INCR, or ALL)
  required: true
  type: choice
  options:
    - ALL
    - INIT
    - INCR
  default: 'ALL'
```

Update the `docker run` command (line 66) to pass it as an argument after the image name:

```yaml
- name: Run Docker container
  run: |
    docker run --rm -v .:/dagster/ \
      -e BVEX=${{ vars.BV_EXCLUDE }} \
      -e PROJECT_NAME=${{ github.event.inputs.project }} \
      -e FMCN=${{ github.event.inputs.FMCN }} \
      -e STLD=${{ github.event.inputs.STDLDT }} \
      -e TZN=${{ github.event.inputs.ScheduleTimezone }} \
      --name kris_container \
      ${{ vars.AWS_ACCOUNT_ID }}.dkr.ecr.${{ vars.AWS_REGION }}.amazonaws.com/${{ vars.ECR_REPO }}:${{ github.event.inputs.VERSN }} \
      --load-type ${{ github.event.inputs.LoadType }}
```

The `--load-type` argument appears after the image name so Docker passes it to `kris.py` as a CLI argument.

---

## How Separate Generation Runs Interact

| Scenario | Command | What Gets Generated | What's Untouched |
|----------|---------|--------------------|--------------------|
| INCR re-export from VaultSpeed | `kris.py --load-type INCR` | `incr_*.py` assets, INCR jobs, INCR schedules, INCR sensors | All `init_*.py` assets, INIT jobs, INIT sensors |
| INIT re-export from VaultSpeed | `kris.py --load-type INIT` | `init_*.py` assets, INIT jobs, INIT sensors | All `incr_*.py` assets, INCR jobs, INCR schedules, INCR sensors |
| New source onboarding | `kris.py --load-type ALL` | Everything for the new source | Existing sources unchanged |
| Full regeneration | `kris.py --load-type ALL` | All artifacts | Custom modules unchanged |

---

## Sensor and Schedule Behavior

With all artifacts in one code location, orchestration remains straightforward:

| Component | Load Type | Behavior |
|-----------|-----------|----------|
| `incr_bv_generator` sensor | INCR | Monitors all INCR raw vault jobs; triggers `incr_bv_job` when all complete. Generated only during INCR runs. |
| INCR failure sensors | INCR | One per INCR job. Generated only during INCR runs. |
| INIT failure sensors | INIT | One per INIT job. Generated only during INIT runs. |
| INCR schedules | INCR | Cron-based (`0 3 * * *`). Generated only during INCR runs. |
| INIT schedules | INIT | None — INIT jobs are manually triggered. |
| Post-BV chain (dynamic tables, validations) | Custom | Not generated — lives in `custom/`. Unaffected by either generation run. |

---

## Verification

1. `python kris.py --load-type ALL` produces identical output to the current `python kris.py` (no behavioral change for default)
2. `python kris.py --load-type INCR` followed by `python kris.py --load-type INIT` produces the same final state as `--load-type ALL`
3. Running the same `--load-type` twice is idempotent (no duplicates)
4. The `incr_bv_generator` sensor still references the correct INCR jobs after an INIT-only run
5. Custom modules (`custom/`) remain completely unaffected

---

## Migration Path

1. **Backward compatible** — `kris.py --load-type ALL` (or no flag) produces the same result as today
2. **No Dagster config changes** — single code location, single `dagster_cloud.yaml` entry
3. **No Snowflake changes** — same procedures, same sequences, same schemas
4. **Rollback** — remove the `--load-type` flag and run `kris.py` as before
5. **Testing** — diff the output of `--load-type ALL` against sequential `--load-type INCR` + `--load-type INIT` to verify equivalence

---

## Future Considerations

- **Source-level filtering**: Add a `--source` flag (`--source KAN`) to regenerate a single source's artifacts without touching others
- **Dry-run mode**: Add `--dry-run` to preview what files would be generated/modified without writing
- **VaultSpeed CI integration**: Trigger `kris.py --load-type INCR` automatically when new INCR FMC exports are committed to the repo
- **INIT as rare operation**: With separate generation, INIT artifacts can be generated once and left untouched for months, reducing the surface area of routine deployments
