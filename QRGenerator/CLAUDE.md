# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Single-script CLI tool: generates QR code PNG from URL. No build, no tests, no framework.

## Commands

```bash
pip install -r requirements.txt      # install deps (qrcode, pillow)
python qr_generator.py <url>         # generate QR code -> qr_code.png
python qr_generator.py <url> --logo logo.png --text "caption" -o out.png
```

## Architecture

`qr_generator.py` is entire app, argparse-driven: builds QR via `qrcode` lib (version 1, error correction H, box_size 10, border 4). `--logo` and `--text` are mutually exclusive (argparse group) — both occupy the same center white quiet-zone box, sized/capped identically. `--logo` pastes a centered image; `--text` shrinks font size until it fits the same size cap, then draws it centered. Output path configurable via `-o/--output` (default `qr_code.png`).

Center content is capped at 1/5 of QR width (incl. padding) — tested empirically with opencv's `QRCodeDetector`. ERROR_CORRECT_H nominally tolerates ~30% obstruction, but a solid center block can wipe entire codeword blocks rather than damage spread evenly, so 1/3 width broke decoding while 1/4-1/8 scanned fine; 1/5 is the current margin of safety. If the center-box size/padding logic changes, re-verify scannability rather than trusting the nominal % alone.
