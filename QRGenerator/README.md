# QR Generator

Simple CLI script that generates QR code PNG from URL.

## Requirements

- Python 3
- `qrcode`, `pillow` (see `requirements.txt`)

## Install

```bash
pip install -r requirements.txt
```

## Usage

```bash
python qr_generator.py <url> [-o OUTPUT] [--logo LOGO] [--text TEXT]
```

- `-o, --output` — output file path (default: `qr_code.png`)
- `--logo` — optional image to overlay in the center of the QR code
- `--text` — optional short text to overlay in the center of the QR code

`--logo` and `--text` are mutually exclusive (both occupy the same center spot).

### Examples

```bash
python qr_generator.py https://forms.gle/bUTssDxYJ6eXzzfN6
python qr_generator.py https://example.com --text "Scan me"
python qr_generator.py https://example.com --logo logo.png -o out.png
```
