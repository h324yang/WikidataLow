import os
from util import (
    load_refs_from_txt,
    load_descriptions,
    write_jsonl
)
import random
import json

LANGS = open("meta/low-langs.txt").read().strip().split()
OUTPUT_DIR = "examples"
NUM_NEG_SAMPLES = 10
RANDOM_SEED = 1234

random.seed(RANDOM_SEED)

def main():
    for lang in LANGS:
        print(lang)
        desc_1 = load_descriptions(f"data/{lang}_en/comment_1")
        desc_2 = load_descriptions(f"data/{lang}_en/comment_2")
        cur_refs = load_refs_from_txt(f"data/{lang}_en/train")
        tails = set([tail for head, tail in cur_refs])

        examples = []
        for center, pos in cur_refs:
            text_center = desc_1[center]
            if pos == "-1":
                text_pos = "NIL"
            else:
                text_pos = desc_2[pos]

            for _ in range(NUM_NEG_SAMPLES):
                while True:
                    neg = random.sample(tails, 1)[0]
                    if neg != pos and neg != "-1":
                        break

                text_neg = desc_2[neg]
                guid = f"{center}-{pos}-{neg}"
                example = {
                    "guid": guid,
                    "text_center": text_center,
                    "text_pos": text_pos,
                    "text_neg": text_neg
                }

                examples.append(example)

        random.shuffle(examples)
        opath = f"{lang}_en.triples.text.train.jsonl"
        write_jsonl(examples, os.path.join(OUTPUT_DIR, opath))

        cur_refs = load_refs_from_txt(f"data/{lang}_en/ref_ent_ids")
        examples = [{"id": "-1", "desc": "NIL"}]
        for head, tail in cur_refs:
            examples.append({"id": head, "desc": desc_1[head]})
            if tail != "-1":
                examples.append({"id": tail, "desc": desc_2[tail]})

        opath = f"{lang}_en.text.ref.jsonl"
        write_jsonl(examples, os.path.join(OUTPUT_DIR, opath))


if __name__ == "__main__":
    main()
