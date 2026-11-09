# Changelog

All notable changes to this project. Format: Keep a Changelog 1.1.
Versions follow Semantic Versioning; no release is tagged yet.

## [Unreleased]

### Changed
- 2026-11-08: Adopted the portfolio standard v2. README opens with the
  question, headline, field, job family and verdict (Functional).
  AUDIT.md is the report; its grade moved out of the public repo
  (grades are internal), and it gained "How the evidence is protected"
  and "Threats to validity". The lock is 027ef11 (tag proposal-v1),
  and a test pins PREREGISTRATION.md by SHA-256. CI checks that
  results/ and RESULTS.md regenerate unchanged from the raw evidence;
  scripts/ack_gap_analysis.py now reads the gzipped ack logs shipped
  in results/ack-logs/ when the machine-of-record work folder is
  absent. CodeQL and Dependabot added; CI token is read-only.

### Fixed
- 2026-11-08: Late fault triggers disclosed. RESULTS.md injection
  notes now print the actual trigger count from each run's events
  against the locked threshold; AUDIT.md records the driver's
  polling lag as a deviation and no longer says the fault schedule
  ran exactly as locked. No verdict changes.

### Added
- 2026-11-08: The failed broker start log behind Amendment A1.
- 2026-11-08: README and AUDIT.
- 2026-11-08: Results: 24 of 24 runs completed; H1, H2, H3 CONFIRMED.
- 2026-11-08: Analysis corrections: longest-ack-gap recomputed from
  the raw logs; per-run injection notes.
- 2026-11-08: Harness: compose-managed broker driver, pipeline stages,
  verifier, results generator, CI.
- 2026-11-08: Amendment A1 (acc54de): machine of record moves to
  BlackBoxV2, broker in Docker; DECISIONS.md D5 supersedes D1.
- 2026-11-08: Pre-registration (027ef11), before any code or run.
