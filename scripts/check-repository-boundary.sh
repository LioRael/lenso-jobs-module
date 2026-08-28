#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-jobs\nlenso-jobs-plugin'
actual_crates="$(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml -print0 | xargs -0 sed -n 's/^name = "\([^"]*\)"/\1/p' | sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  echo "unexpected workspace crate boundary" >&2
  diff -u <(printf '%s\n' "$expected_crates") <(printf '%s\n' "$actual_crates") || true
  exit 1
fi
if rg -n '/Users/' --glob '*.md' --glob '*.toml' --glob '*.yml' --glob '*.yaml' .; then
  echo "public files contain a machine-local absolute path" >&2
  exit 1
fi
