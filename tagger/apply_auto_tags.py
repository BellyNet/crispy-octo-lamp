import os

IMAGES_DIR = r"F:\Dev\LoRA-Training\tagger\final"

# Static tag set for all images
TAGS = [
    "bbw", "obese", "superfat", "big_belly", "hanging_belly",
    "belly_folds", "wide_hips", "thick_thighs", "soft_arms",
    "curvy", "plush_body", "stuffed_belly", "gluttonous",
    "feedee", "round_gut", "plus_size"
]

def apply_tags(images_dir, tags):
    txt_files = [f for f in os.listdir(images_dir) if f.lower().endswith(".txt")]
    for txt_file in txt_files:
        txt_path = os.path.join(images_dir, txt_file)
        with open(txt_path, "r", encoding="utf-8") as f:
            caption = f.read().strip()
        combined = f"{', '.join(tags)}, {caption}"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(combined)
        print(f"✅ Updated {txt_file} with tags")

if __name__ == "__main__":
    apply_tags(IMAGES_DIR, TAGS)
    print("\n✅ All captions updated with static tags. Ready for LoRA training!")
