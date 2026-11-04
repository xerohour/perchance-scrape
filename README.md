# Perchance scrape workflow

The Generator Scraper's zip pipeline decodes binaries as text — every PNG,
WASM, OGG, font, and archive it touches arrives corrupted. This workflow
replaces the manual rescue pass: binaries never come from the zip at all.

## Steps for each new port

1. Run the Generator Scraper, download `all.zip`, unzip into a workspace dir.
2. Run the binary step:
   ```
   python3 ~/workspace/perchance-scrape/fetch_binaries.py \
       --generator <slug> --src <extracted-dir>/src
   ```
   Every binary in the generator's `srcManifest` is downloaded straight from
   Perchance's file CDN (`https://user.uploads.dev/file/<key>`) and
   size-verified against the manifest before being written into the tree.
   Text files are size-checked against the manifest and reported, not
   re-downloaded.
3. The step exits non-zero and names every offender if any binary fails to
   verify — do not start port work until it prints OK.
4. Useful flags: `--manifest-file` (reuse a saved manifest / test offline),
   `--dry-run` (report what would be fetched), `--workers N` (parallelism).

## Why this exists

Hit twice before this was built: webscape's 1,706 PNGs and Veloren's 187
binaries (including the 69 MB WASM) all arrived corrupted from scraper zips
and had to be re-downloaded by hand. The old per-port script
(`veloren-port/verify_and_fix.py`) only repaired after the fact; this step
fetches binaries from the CDN up front so the corruption never enters the
tree.

## Manifest scheme

`POST https://perchance.org/api/getGeneratorPageData`
with `{"generatorName": "<slug>"}` returns
`generator.srcManifest`: `{ "path/in/src": {"key": "<cdn-key>", "size": N} }`.
CDN file URL: `https://user.uploads.dev/file/<key>`.
