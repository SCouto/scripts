import qrcode
import sys

# URL to encode
if len(sys.argv) < 2:
      print("Usage: python qr_generator.py <url>")
      sys.exit(1)

url = sys.argv[1]
#url = "https://forms.gle/bUTssDxYJ6eXzzfN6"

# Generate QR code
qr = qrcode.QRCode(
    version=1,
    error_correction=qrcode.constants.ERROR_CORRECT_H,
    box_size=10,
    border=4,
)
qr.add_data(url)
qr.make(fit=True)

# Create an image
img = qr.make_image(fill_color="black", back_color="white")

# Save file
file_path = "qr_code.png"
img.save(file_path)

file_path

