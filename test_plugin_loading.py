#!/usr/bin/env python
"""Diagnostic script to test dvue plugin auto-loading."""

import sys

print("=" * 70)
print("DVue Plugin Auto-Loading Diagnostic")
print("=" * 70)

# Step 1: Check entry points
print("\n[1] Checking entry points discovery...")
try:
    from importlib.metadata import entry_points
    eps = entry_points()
    group = eps.select(group='dvue.plugins') if hasattr(eps, 'select') else eps.get('dvue.plugins', [])
    print(f"   ✓ Found {len(group)} dvue plugins")
    for ep in group:
        print(f"     • {ep.name:20} → {ep.value}")
except Exception as e:
    print(f"   ✗ Failed: {e}")
    sys.exit(1)

# Step 2: Check if dsm2ui.readers can be imported
print("\n[2] Checking dsm2ui.readers module...")
try:
    import dsm2ui.readers
    print(f"   ✓ dsm2ui.readers imported successfully")
    print(f"     • register_readers function: {hasattr(dsm2ui.readers, 'register_readers')}")
except Exception as e:
    print(f"   ✗ Failed to import: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 3: Call the registry loader
print("\n[3] Loading plugins via ReaderRegistry...")
try:
    from dvue.registry import ReaderRegistry
    loaded = ReaderRegistry.load_plugins_from_entry_points()
    print(f"   ✓ Loaded {len(loaded)} plugins: {loaded}")
except Exception as e:
    print(f"   ✗ Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Check registered readers
print("\n[4] Checking registered readers...")
try:
    readers = ReaderRegistry.get_registered_readers()
    extensions = ReaderRegistry.get_registered_extensions()
    print(f"   ✓ Found {len(readers)} registered ref_types and {len(extensions)} extensions")
    print("\n   Readers:")
    for ref_type in sorted(readers.keys()):
        exts_for_type = [e for e, c in extensions.items() if c is readers[ref_type]]
        if exts_for_type:
            print(f"     • {ref_type:20} → {readers[ref_type].__name__:30} {sorted(exts_for_type)}")
        else:
            print(f"     • {ref_type:20} → {readers[ref_type].__name__:30}")
except Exception as e:
    print(f"   ✗ Failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "=" * 70)
print("✓ All checks passed! Plugins are auto-loading correctly.")
print("=" * 70)
