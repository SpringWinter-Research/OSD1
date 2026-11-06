---
title: Security scan
---

{% include nav.html %}

# Security scan

`./scripts/scan.sh` records dependency advisories and image advisories. A finding does not fail the script. A scanner that does not run does. This page quotes a run from 2026-11-06. The image is not clean.

## Cargo dependencies

`cargo-audit` 0.22.2. Unedited scan lines:

```
=== cargo audit ===
    Fetching advisory database from `https://github.com/RustSec/advisory-db.git`
      Loaded 1409 security advisories (from /Users/angsak/.cargo/advisory-db)
    Updating crates.io index
    Scanning Cargo.lock for vulnerabilities (77 crate dependencies)
cargo_audit_exit 0
```

Exit 0 and no advisory records means the RustSec database had no reported vulnerability for those 77 dependencies on that day.

## Container image

Trivy image: `aquasec/trivy:latest`, digest `sha256:8f6bb947dbc528902e11f2f5be52f7b519f6a39d5d148f41166f2040499768fb`. The log refers to Trivy documentation for v0.76. Target: `osd1-osd1:latest` (debian 12.15), 93 OS packages, 0 language-specific files. Rust crate advisories are the Cargo section above, because the image contains the compiled binary.

```
Total: 225 (UNKNOWN: 0, LOW: 85, MEDIUM: 103, HIGH: 36, CRITICAL: 1)
trivy_exit 0
```

The critical row:

```
zlib1g  CVE-2023-45853  CRITICAL  will_not_fix  1:1.2.13.dfsg-1
zlib: integer overflow and resultant heap-based buffer overflow in zipOpenNewFileInZip4_64
```

The other 224 rows are Debian 12.15 packages in the runtime image, including util-linux, wget, ncurses, systemd, perl, gzip, and acl at high severity. Re-run `./scripts/scan.sh` to print the full table.
