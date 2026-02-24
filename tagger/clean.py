import os

# path to your captions folder
CAPTIONS_DIR = r"F:\Dev\LoRA-Training\tagger\final"

for filename in os.listdir(CAPTIONS_DIR):
    if filename.endswith(".txt"):
        path = os.path.join(CAPTIONS_DIR, filename)
        with open(path, "r", encoding="utf-8") as f:
            text = f.read().strip()

        # replace underscores with spaces
        text = text.replace("_", " ")

        # add queenAva if it's not already there
        if "queenAva" not in text:
            text = f"queenAva, {text}"

        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

print("✅ All captions updated with queenAva and underscores fixed.")
