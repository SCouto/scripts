import argparse

import qrcode
from PIL import Image, ImageDraw, ImageFont

parser = argparse.ArgumentParser(description="Generate a QR code, optionally with a center logo and caption text.")
parser.add_argument("url", help="URL (or any text) to encode")
parser.add_argument("-o", "--output", default="qr_code.png", help="output file path (default: qr_code.png)")
group = parser.add_mutually_exclusive_group()
group.add_argument("--logo", help="path to logo image to overlay in the center")
group.add_argument("--text", help="short text to overlay in the center")
args = parser.parse_args()

# Generate QR code
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_H,
    box_size=10,
    border=4,
)
qr.add_data(args.url)
qr.make(fit=True)

img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

if args.logo:
    logo = Image.open(args.logo).convert("RGBA")
    # Keep logo+padding well under the ~30% obstruction ERROR_CORRECT_H tolerates;
    # a solid center block wipes whole codeword blocks, so 1/4 of width (incl. padding) is the safe max.
    max_logo_size = img.size[0] // 5
    logo.thumbnail((max_logo_size, max_logo_size))

    # White quiet-zone box behind logo, slightly bigger than the logo itself.
    pad = max(logo.size) // 8
    box_size = (logo.size[0] + pad * 2, logo.size[1] + pad * 2)
    box_pos = ((img.size[0] - box_size[0]) // 2, (img.size[1] - box_size[1]) // 2)
    draw = ImageDraw.Draw(img)
    draw.rectangle([box_pos, (box_pos[0] + box_size[0], box_pos[1] + box_size[1])], fill="white")

    logo_pos = ((img.size[0] - logo.size[0]) // 2, (img.size[1] - logo.size[1]) // 2)
    img.paste(logo, logo_pos, mask=logo)

if args.text:
    # Same safe max size as --logo: keep white box under ~1/5 of QR width.
    max_box_size = img.size[0] // 5

    # Shrink font until text (plus padding) fits within max_box_size.
    font_size = 28
    while font_size > 8:
        font = ImageFont.load_default(size=font_size)
        draw = ImageDraw.Draw(img)
        text_bbox = draw.textbbox((0, 0), args.text, font=font)
        text_w, text_h = text_bbox[2] - text_bbox[0], text_bbox[3] - text_bbox[1]
        pad = max(text_w, text_h) // 8
        if text_w + pad * 2 <= max_box_size and text_h + pad * 2 <= max_box_size:
            break
        font_size -= 2

    box_size = (text_w + pad * 2, text_h + pad * 2)
    box_pos = ((img.size[0] - box_size[0]) // 2, (img.size[1] - box_size[1]) // 2)
    draw.rectangle([box_pos, (box_pos[0] + box_size[0], box_pos[1] + box_size[1])], fill="white")

    text_pos = ((img.size[0] - text_w) // 2 - text_bbox[0], (img.size[1] - text_h) // 2 - text_bbox[1])
    draw.text(text_pos, args.text, fill="black", font=font)

img.save(args.output)
print(f"Saved QR code to {args.output}")

