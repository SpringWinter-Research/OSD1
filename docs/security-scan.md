---
title: Security scan
description: Recorded cargo-audit and Trivy results. The Trivy rows are the Debian 12.15 image.
---

# Security scan

`./scripts/scan.sh` records dependency advisories and image advisories. A finding does not fail the script. A scanner that does not run does fail the script. This page quotes the recorded run against the Debian 12.15 image `osd1-osd1:latest`. It does not quote a scan of the distroless `latest` or `2026-11-26` image. That recorded image is not clean.

## Cargo dependencies

`cargo-audit` 0.22.2. Recorded scan lines:

```
=== cargo audit ===
    Fetching advisory database from `https://github.com/RustSec/advisory-db.git`
      Loaded 1409 security advisories (from /Users/angsak/.cargo/advisory-db)
    Updating crates.io index
    Scanning Cargo.lock for vulnerabilities (77 crate dependencies)
cargo_audit_exit 0
```

Exit 0 and no advisory records means the RustSec database had no reported vulnerability for those 77 dependencies in that run.

## Container image

Trivy image: `aquasec/trivy:latest`, digest `sha256:8f6bb947dbc528902e11f2f5be52f7b519f6a39d5d148f41166f2040499768fb`. The log refers to Trivy documentation for v0.76. Target: `osd1-osd1:latest` (Debian 12.15), 93 OS packages, 0 language-specific files. Rust crate advisories are the Cargo section above, because the image contains the compiled binary.

```
Total: 225 (UNKNOWN: 0, LOW: 85, MEDIUM: 103, HIGH: 36, CRITICAL: 1)
trivy_exit 0
```

The critical row:

```
zlib1g  CVE-2023-45853  CRITICAL  will_not_fix  1:1.2.13.dfsg-1
zlib: integer overflow and resultant heap-based buffer overflow in zipOpenNewFileInZip4_64
```

The other 224 rows are Debian 12.15 packages in that recorded image. They include util-linux, wget, ncurses, systemd, perl, gzip, and acl at high severity. Run `./scripts/scan.sh` again to print the full table.

Access rules for the token, the raft secret, and `/metrics` are on the [system design](system-design.md) page.
