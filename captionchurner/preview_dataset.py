import os

# 📂 Dataset folder (same level as 'dataset/')
base_folder = os.path.join(os.getcwd(), "dataset")
output_file = os.path.join(os.getcwd(), "dataset_preview.txt")

with open(output_file, "w", encoding="utf-8") as out:
    for model_folder in os.listdir(base_folder):
        model_path = os.path.join(base_folder, model_folder)
        caption_folder = os.path.join(model_path, "captions")
        tag_folder = os.path.join(model_path, "tags")

        if not os.path.isdir(caption_folder) or not os.path.isdir(tag_folder):
            continue

        for caption_file in os.listdir(caption_folder):
            if not caption_file.endswith(".txt"):
                continue

            base_name = os.path.splitext(caption_file)[0]
            caption_path = os.path.join(caption_folder, caption_file)
            tag_path = os.path.join(tag_folder, f"{base_name}.txt")

            image_name = base_name + ".jpg"  # assuming JPGs — tweak if needed

            try:
                with open(caption_path, "r", encoding="utf-8") as cf:
                    caption = cf.read().strip()
                with open(tag_path, "r", encoding="utf-8") as tf:
                    tags = tf.read().strip()
            except Exception as e:
                print(f"❌ Couldn’t read {base_name}: {e}")
                continue

            out.write(f"🔹 Model: {model_folder}\n")
            out.write(f"📸 Image: {image_name}\n")
            out.write(f"🍼 Caption: {caption}\n")
            out.write(f"🏷️ Tags: {tags}\n\n")

print("✅ Preview saved as dataset_preview.txt")
