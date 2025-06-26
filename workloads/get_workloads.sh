#!/usr/bin/env sh
# Fetch FAFO workload archives into this directory.

set -e

# Change to script directory so files land here
cd "$(dirname "$0")"

# List of files to download (one per line)
URLS="
https://lz-research-public.s3.us-west-2.amazonaws.com/2025-June-FAFO/eth.ndjson.gz
https://lz-research-public.s3.us-west-2.amazonaws.com/2025-June-FAFO/token_erc20.ndjson.gz
"

# Name of the checksum manifest written in this directory
CHECKSUM_FILE="checksums.md5"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

need_downloader() {
  if command -v curl >/dev/null 2>&1; then
    echo curl
  elif command -v wget >/dev/null 2>&1; then
    echo wget
  else
    echo "Error: need curl or wget installed" >&2
    return 1
  fi
}

download() {
  url="$1"
  file="$(basename "$url")"

  # Determine expected checksum if manifest present
  expected="$(expected_md5 "$file")"

  # If file exists, verify checksum (if we have expected)
  if [ -f "$file" ]; then
    if [ -n "$expected" ]; then
      actual="$(compute_md5 "$file")"
      if [ "$actual" = "$expected" ]; then
        echo "✓ $file present – checksum OK – skip"
        # Ensure decompressed copy exists (no-op if already there)
        decompress_if_needed "$file"
        return 0
      else
        echo "! $file checksum mismatch – redownloading" >&2
        rm -f "$file"
      fi
    else
      echo "! $file present but not in manifest – redownloading to be safe" >&2
      rm -f "$file"
    fi
  fi

  tmp="$file.part"
  rm -f "$tmp"

  downloader="$(need_downloader)" || return 1

  echo "→ Downloading $file ..."

  if [ "$downloader" = "curl" ]; then
    # --fail      : fail on HTTP errors (no body output)
    # --location  : follow redirects
    # --silent    : no progress meter
    # --show-error: show errors even when silent
    if ! curl --fail --location --silent --show-error --output "$tmp" "$url"; then
      echo "✗ Failed to download $url" >&2
      rm -f "$tmp"
      return 1
    fi
  else
    # wget
    # --quiet : silence progress
    if ! wget --quiet --output-document="$tmp" "$url"; then
      echo "✗ Failed to download $url" >&2
      rm -f "$tmp"
      return 1
    fi
  fi

  mv "$tmp" "$file"
  echo "✓ $file downloaded"

  # Verify checksum against manifest
  if [ -n "$expected" ]; then
    actual="$(compute_md5 "$file")"
    if [ "$actual" != "$expected" ]; then
      echo "✗ $file checksum mismatch after download" >&2
      rm -f "$file"
      return 1
    fi
  fi

  # Decompress .gz to plain file if not yet present
  decompress_if_needed "$file"
}

# -----------------------------------------------------------------------------
# Checksum helpers
# -----------------------------------------------------------------------------

# compute_md5 FILE -> echoes the hex MD5 digest to stdout
compute_md5() {
  if command -v md5sum >/dev/null 2>&1; then
    md5sum "$1" | awk '{print $1}'
  elif command -v md5 >/dev/null 2>&1; then # macOS
    md5 -q "$1"
  else
    echo "Error: need md5sum or md5 installed" >&2
    return 1
  fi
}

# expected_md5 FILE -> echoes expected digest from manifest (empty if not found)
expected_md5() {
  [ -f "$CHECKSUM_FILE" ] || return 0
  awk -v f="$1" '$2 == f {print $1; exit}' "$CHECKSUM_FILE"
}

# Decompress FILE if it ends with .gz and the target is missing.
decompress_if_needed() {
  gz="$1"
  case "$gz" in
    *.gz)
      out="${gz%.gz}"
      if [ -f "$out" ]; then
        return 0
      fi
      echo "→ Decompressing $gz → $out"
      if command -v gzip >/dev/null 2>&1; then
        if ! gzip -dc "$gz" >"$out"; then
          echo "✗ Failed to decompress $gz" >&2
          rm -f "$out"
          return 1
        fi
      elif command -v gunzip >/dev/null 2>&1; then
        if ! gunzip -c "$gz" >"$out"; then
          echo "✗ Failed to decompress $gz" >&2
          rm -f "$out"
          return 1
        fi
      else
        echo "Error: need gzip/gunzip to decompress $gz" >&2
        return 1
      fi
      ;;
    *) ;;
  esac
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

for url in $URLS; do
  download "$url"
done

echo "All done – checksums verified."
