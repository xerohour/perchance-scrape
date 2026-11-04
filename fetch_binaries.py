#!/usr/bin/env python3
"""Perchance scrape workflow — binary step.

The Generator Scraper's zip pipeline decodes binaries as text, corrupting
every PNG/WASM/OGG/etc. it touches. Don't trust the zip for binaries at all:
after unzipping, run

    fetch_binaries.py --generator <slug> --src <extracted-src-dir>

For every binary in the generator's srcManifest this downloads the file
straight from Perchance's file CDN (https://user.uploads.dev/file/<key>),
size-verifies it against the manifest, and only then writes it into the tree.
Text files are size-checked against the manifest and reported, not
re-downloaded.

Any binary whose downloaded size doesn't match the manifest fails LOUDLY
(non-zero exit, explicit listing). The port must not proceed until this step
is green.

Manifest source: POST https://perchance.org/api/getGeneratorPageData
{"generatorName": "<slug>"} -> generator.srcManifest: {relpath: {key, size}}.
Use --manifest-file to reuse a saved manifest (testing / offline re-runs).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

CDN_BASE = "https://user.uploads.dev/file/"
PAGEDATA_URL = "https://perchance.org/api/getGeneratorPageData"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# Extensions we treat as text (size-checked against the local copy only).
# Everything else — including extensionless files — is treated as binary:
# downloaded from the CDN and size-verified, never trusted from the zip.
TEXT_EXTS = {
    "js", "mjs", "cjs", "ts", "mts", "cts", "jsx", "tsx",
    "html", "htm", "xhtml", "css", "scss", "sass", "less",
    "json", "json5", "jsonc", "txt", "text", "md", "markdown", "mdx",
    "xml", "xsl", "svg", "yml", "yaml", "toml", "ini", "cfg", "conf",
    "config", "env", "properties",
    "rs", "py", "pyi", "sh", "bash", "zsh", "sql", "ron", "lock", "nix",
    "pjs", "glsl", "vert", "frag", "geom", "comp",
    "csv", "tsv", "log", "gitignore", "gitattributes", "editorconfig",
    "dockerfile", "makefile", "cmake", "gql", "graphql",
    "vue", "svelte", "astro", "php", "rb", "java", "c", "h",
    "cpp", "hpp", "cc", "cs", "go", "swift", "kt", "kts", "lua",
    "pl", "pm", "r", "scala", "hs", "ex", "exs", "erl", "clj",
    "dart", "elm", "ml", "mli", "ps1", "bat", "cmd", "vbs",
    "awk", "sed", "tcl", "vim", "diff", "patch",
}


def is_binary(relpath):
    base = relpath.rsplit("/", 1)[-1]
    if "." not in base:
        return True
    return base.rsplit(".", 1)[-1].lower() not in TEXT_EXTS


def load_manifest(args):
    if args.manifest_file:
        with open(args.manifest_file, encoding="utf-8") as f:
            data = json.load(f)
    else:
        payload = json.dumps({"generatorName": args.generator}).encode()
        r = subprocess.run(
            ["curl", "-s", "--max-time", "60", "-A", UA,
             "-X", "POST", PAGEDATA_URL,
             "-H", "Content-Type: application/json",
             "--data-binary", "@-"],
            input=payload, capture_output=True)
        if r.returncode != 0:
            sys.exit(f"FATAL: could not reach {PAGEDATA_URL} (curl exit {r.returncode})")
        try:
            data = json.loads(r.stdout.decode("utf-8"))
        except json.JSONDecodeError:
            sys.exit("FATAL: page-data endpoint did not return JSON")
    # Accept either the full page-data response or a bare srcManifest dict.
    if isinstance(data, dict) and "srcManifest" in data.get("generator", {}):
        manifest = data["generator"]["srcManifest"]
    elif isinstance(data, dict) and data and all(
            isinstance(v, dict) and "key" in v and "size" in v
            for v in data.values()):
        manifest = data
    else:
        sys.exit("FATAL: could not find srcManifest in manifest data")
    if not manifest:
        sys.exit("FATAL: manifest is empty")
    return manifest


def fetch_one(args_tuple):
    """Download one binary from the CDN into place. Returns (rel, status, detail)."""
    rel, entry, src_dir, dry_run = args_tuple
    key, want = entry["key"], entry["size"]
    local = os.path.join(src_dir, rel)
    if dry_run:
        return (rel, "would-fetch", f"key={key} size={want}")
    os.makedirs(os.path.dirname(local), exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".cdn-dl-", dir=os.path.dirname(local))
    os.close(fd)
    try:
        r = subprocess.run(
            ["curl", "-s", "-A", UA, "--max-time", "600", "--retry", "3",
             "--retry-all-errors", "-o", tmp, f"{CDN_BASE}{key}"],
            capture_output=True)
        if r.returncode != 0:
            return (rel, "FAILED", f"curl exit {r.returncode}")
        got = os.path.getsize(tmp)
        if got != want:
            return (rel, "FAILED",
                    f"SIZE MISMATCH after CDN download: got {got}, manifest wants {want}")
        os.replace(tmp, local)
        return (rel, "ok", f"{want} bytes")
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--generator", help="Perchance generator slug (e.g. veloren)")
    ap.add_argument("--src", required=True,
                    help="Extracted src dir; manifest paths are relative to it")
    ap.add_argument("--manifest-file",
                    help="Use a saved manifest JSON instead of the live API")
    ap.add_argument("--workers", type=int, default=6,
                    help="Parallel CDN downloads (default 6)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would be fetched; download nothing")
    args = ap.parse_args()
    if not args.generator and not args.manifest_file:
        ap.error("--generator or --manifest-file is required")

    src_dir = os.path.abspath(os.path.expanduser(args.src))
    os.makedirs(src_dir, exist_ok=True)

    manifest = load_manifest(args)
    print(f"manifest entries: {len(manifest)}")

    binaries = [(rel, e) for rel, e in manifest.items() if is_binary(rel)]
    texts = [(rel, e) for rel, e in manifest.items() if not is_binary(rel)]
    print(f"binaries to fetch from CDN: {len(binaries)}")
    print(f"text files to size-check locally: {len(texts)}")

    ext_counter = Counter(
        rel.rsplit("/", 1)[-1].rsplit(".", 1)[-1].lower() if "." in rel.rsplit("/", 1)[-1] else "(noext)"
        for rel, _ in binaries)
    print("binary types:", dict(ext_counter.most_common()))

    # --- binary step: CDN download + size verify ---
    failed = []
    fetched_ok = 0
    total_bytes = 0
    if binaries:
        tasks = [(rel, e, src_dir, args.dry_run) for rel, e in binaries]
        done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for rel, status, detail in ex.map(fetch_one, tasks):
                done += 1
                if status == "ok":
                    fetched_ok += 1
                    total_bytes += manifest[rel]["size"]
                elif status == "FAILED":
                    failed.append((rel, detail))
                if done % 25 == 0 or done == len(tasks):
                    print(f"  ...{done}/{len(tasks)} binaries", flush=True)

    # --- text step: local size check only ---
    text_ok, text_warn, text_missing = [], [], []
    if not args.dry_run:
        for rel, e in texts:
            local = os.path.join(src_dir, rel)
            if not os.path.exists(local):
                text_missing.append(rel)
            elif os.path.getsize(local) != e["size"]:
                text_warn.append((rel, e["size"], os.path.getsize(local)))
            else:
                text_ok.append(rel)

    # local files with no manifest entry (informational)
    local_extra = []
    for root, _, files in os.walk(src_dir):
        for f in files:
            if f.startswith(".cdn-dl-"):
                continue
            rel = os.path.relpath(os.path.join(root, f), src_dir)
            if rel not in manifest:
                local_extra.append(rel)

    print("---- results ----")
    print(f"binaries fetched+verified: {fetched_ok}/{len(binaries)} "
          f"({total_bytes} bytes, {total_bytes/1e6:.1f} MB)")
    if not args.dry_run:
        print(f"text size-ok: {len(text_ok)}, size-mismatch (warning): {len(text_warn)}, "
              f"missing local: {len(text_missing)}")
        for rel, want, got in text_warn[:10]:
            print(f"  TEXT WARN {rel}: want {want}, got {got}")
        for rel in text_missing[:10]:
            print(f"  TEXT MISSING {rel}")
    print(f"local files not in manifest: {len(local_extra)}")
    for rel in sorted(local_extra)[:10]:
        print(f"  EXTRA {rel}")

    if failed:
        print()
        print("=" * 70)
        print(f"BINARY VERIFY FAILED: {len(failed)} file(s) did not verify")
        print("=" * 70)
        for rel, detail in failed:
            print(f"  FAIL {rel}: {detail}")
        print("The port must NOT proceed until every binary verifies.")
        sys.exit(1)

    print()
    print(f"OK: all {fetched_ok} binaries fetched from CDN and size-verified.")
    if args.dry_run:
        print("(dry run — nothing was downloaded)")


if __name__ == "__main__":
    main()
