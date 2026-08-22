# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Grab-bag of standalone ops/infra scripts (AWS, Confluent Cloud, Databricks). No shared library, no build system, no test suite, no package manifest tying them together — each subdirectory is independent with its own `requirements.txt` where needed. Treat each script as its own small tool; don't invent shared abstractions across them.

## Layout & commands

- `s3/s3_bucket_sizes_fast.py` — no-arg: lists all bucket sizes via CloudWatch metrics (fast, parallel, 24-48h stale). `--bucket NAME [--prefix P]`: enumerates objects and sizes by top-level subfolder (slow, exact). Requires `boto3` + AWS credentials.
- `s3/s3_delete_paths.sh [--execute] [--file paths_file]` — deletes S3 URIs listed one-per-line in `s3/paths_to_delete.txt` (default file, gitignored — it's a working scratch list, not tracked). **Dry-run by default**; pass `--execute` to actually run `aws s3 rm --recursive`.
- `ec2/ec2_connect.py [--profile NAME]` — interactive picker (via `inquirer`) to SSO-login and connect to Airflow EC2 instances (scheduler/worker/triggerer/webserver/dag_processor/api_server) over SSM. Single selection connects in the current pane; multiple selections open iTerm split panes (macOS/iTerm-only, uses `osascript`). Defaults to AWS profile `dev`. Install deps: `pip install -r ec2/requirements.txt`.
- `confluent/script.sh [start_date] [end_date]` — Confluent Cloud billing costs by resource/product for a date range (default: last 30 days), via `confluent` CLI + `jq`. Requires `confluent` CLI login.
- `confluent/run_daily.sh <init_date> <end_date> <grep_pattern> [output_file]` — calls `script.sh` once per day in the range, filters output by `grep_pattern`, appends to `output_file` (default `cost.txt`).
- `grant_schema_permissions.py` — Databricks notebook (run via Databricks, not plain Python — uses the injected `spark` session). Edit the `schema` and `principals` list at the top, then run cells to GRANT read/write privilege sets on a UC schema per principal.
- `QRGenerator/` — standalone QR code generator CLI; see `QRGenerator/CLAUDE.md` for details (own script, own requirements, own README).

## Working with these scripts

- No linter/formatter/test runner is configured anywhere in the repo — verify changes by running the script directly against real (or dry-run) AWS/Confluent/Databricks resources.
- Scripts that touch cloud state (`s3_delete_paths.sh`, `ec2_connect.py`, `grant_schema_permissions.py`) are destructive or credential-sensitive — respect existing dry-run flags and confirm intent before making them execute for real.
- macOS-specific bits exist (`osascript`/iTerm in `ec2_connect.py`, `date -j` branches in `confluent/run_daily.sh`) — preserve the Linux fallback branches when editing.
