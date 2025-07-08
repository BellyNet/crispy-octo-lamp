import os
from PIL import Image
import torch
from transformers import AutoProcessor, LlavaForConditionalGeneration

# 🐮 Model setup
model_id = "llava-hf/llava-1.5-7b-hf"
device = "cuda" if torch.cuda.is_available() else "cpu"

print("📥 Loading LLaVA model...")
model = LlavaForConditionalGeneration.from_pretrained(
    model_id,
    torch_dtype=torch.float16,
    low_cpu_mem_usage=True,
    device_map="auto"
)
processor = AutoProcessor.from_pretrained(model_id)

# 🐖 Where your girls live
base_folder = os.path.join(os.getcwd(), "milkmaid", "dataset")

# 💬 Generate caption using LLaVA
def churn_caption(image_path):
    print(f"🔍 Attempting to process image: {image_path}")
    if not os.path.exists(image_path):
        print(f"❌ ERROR: File does not exist: {image_path}")
        return None

    try:
        image = Image.open(image_path).convert("RGB")
        prompt = "Describe this image in detail with NSFW emphasis on body features and body type."
        inputs = processor(image, prompt, return_tensors="pt").to(device, torch.float16)
        output = model.generate(**inputs, max_new_tokens=120)
        caption = processor.batch_decode(output, skip_special_tokens=True)[0]
        return caption
    except Exception as e:
        print(f"❌ Failed processing {image_path}: {e}")
        return None

# 🐄 Iterate all model folders
print(f"🔎 Scanning base folder: {base_folder}")
for model_folder in os.listdir(base_folder):
    model_path = os.path.join(base_folder, model_folder)
    image_folder = os.path.join(model_path, "images")
    caption_folder = os.path.join(model_path, "captions")

    print(f"\n🧷 Model: {model_folder}")
    print(f"📂 Image folder: {image_folder}")
    print(f"📂 Caption folder: {caption_folder}")

    if not os.path.isdir(image_folder):
        print(f"⚠️  Skipping {model_folder} (no images folder found)")
        continue

    os.makedirs(caption_folder, exist_ok=True)
    print(f"🍼 Milking captions for: {model_folder}")

    for filename in os.listdir(image_folder):
        if not filename.lower().endswith((".jpg", ".jpeg", ".png")):
            print(f"⛔ Skipped non-image file: {filename}")
            continue

        base_name = os.path.splitext(filename)[0]
        image_path = os.path.join(image_folder, filename)
        caption_path = os.path.join(caption_folder, f"{base_name}.txt")

        print(f"📸 Image: {filename}")
        print(f"📍 Full path: {image_path}")
        print(f"📝 Caption path: {caption_path}")

        if os.path.exists(caption_path):
            print(f"🥱 Already churned, skipping: {filename}")
            continue

        caption = churn_caption(image_path)
        if caption:
            with open(caption_path, "w", encoding="utf-8") as f:
                f.write(caption)
            print(f"💬 Captioned: {filename}")
        else:
            print(f"💨 Skipped due to error: {filename}")

print("\n✅ All thicc captions churned with LLaVA.")
