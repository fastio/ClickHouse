#!/usr/bin/env python3
"""Post-vendor patch for the Lance integration.

`lance-arrow` declares `crate-type = ["cdylib", "rlib"]`. cargo builds the cdylib even though
ClickHouse only links the rlib (via the `_ch_rust_lance` staticlib). That throwaway cdylib fails
to link against ClickHouse's hermetic sysroot, which lacks `-lgcc_s`/`-lutil`. We drop "cdylib"
so only the rlib is built, and fix the vendored checksum so the offline/locked build accepts it.

Must be re-run after every `./rust/vendor.sh`.
"""
import hashlib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VENDOR = os.environ.get("RUST_VENDOR_DIR", os.path.join(ROOT, "contrib", "rust_vendor"))
CRATE = os.path.join(VENDOR, "lance-arrow-2.0.1")
CARGO = os.path.join(CRATE, "Cargo.toml")
CHECKSUM = os.path.join(CRATE, ".cargo-checksum.json")

if not os.path.exists(CARGO):
    print("lance-arrow-2.0.1 is not vendored; nothing to patch.")
    sys.exit(0)

text = open(CARGO, encoding="utf-8").read()
needle = '    "cdylib",\n    "rlib",\n'
replacement = '    "rlib",\n'

if needle in text:
    text = text.replace(needle, replacement)
    open(CARGO, "w", encoding="utf-8").write(text)
    print("Patched lance-arrow Cargo.toml: removed 'cdylib' from crate-type.")
elif '"cdylib"' in text:
    print("WARNING: lance-arrow crate-type has 'cdylib' in an unexpected layout; not patched.")
    sys.exit(1)
else:
    print("lance-arrow crate-type already patched (no 'cdylib').")

new_sha = hashlib.sha256(open(CARGO, "rb").read()).hexdigest()
data = json.load(open(CHECKSUM, encoding="utf-8"))
if data["files"].get("Cargo.toml") != new_sha:
    data["files"]["Cargo.toml"] = new_sha
    json.dump(data, open(CHECKSUM, "w", encoding="utf-8"))
    print("Updated .cargo-checksum.json: Cargo.toml ->", new_sha)
else:
    print("Checksum already up to date.")
