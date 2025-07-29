import os
from PIL import Image

# CONFIG
INPUT_DIR = r"F:/Dev/LoRA-Training/tagger/final"
OUTPUT_DIR = INPUT_DIR  # overwrite in place, or set a different folder
TARGET_SIZE = (768, 768)
BACKGROUND_COLOR = (0, 0, 0)  # black background

def resize_and_pad(img, size, color=(0, 0, 0)):
    """Resize while maintaining aspect ratio, then pad to target size."""
    img.thumbnail(size, Image.LANCZOS)
    new_img = Image.new("RGB", size, color)
    new_img.paste(img, ((size[0] - img.width) // 2, (size[1] - img.height) // 2))
    return new_img

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

count = 0
for file in os.listdir(INPUT_DIR):
    if file.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
        path = os.path.join(INPUT_DIR, file)
        try:
            img = Image.open(path).convert("RGB")
            img_resized = resize_and_pad(img, TARGET_SIZE, BACKGROUND_COLOR)
            out_path = os.path.join(OUTPUT_DIR, os.path.splitext(file)[0] + ".jpg")
            img_resized.save(out_path, "JPEG", quality=95)
            count += 1
        except Exception as e:
            print(f"Failed to process {file}: {e}")

print(f"✅ Done! Resized {count} images to {TARGET_SIZE}.")
