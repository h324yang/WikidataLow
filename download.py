import argparse
import os
import sys
import wget


TARGET_DATE = "20200901"
LANGS = open("./meta/low-langs.txt").read().split() + ["en"] 
BASE_URL = 'https://dumps.wikimedia.org/{lang}wiki/{date}/'
FILES = (
    '{lang}wiki-{date}-page_props.sql.gz',
)
OUTPUT_DIR = "./dumps"


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
        
    for lang in LANGS:
        for file in FILES:
            file = file.format(lang=lang, date=TARGET_DATE)
            base_url = BASE_URL.format(lang=lang, date=TARGET_DATE)
            url = os.path.join(base_url, file)
            print(url)
            wget.download(url, out=OUTPUT_DIR)
    
    
if __name__ == "__main__":
    main()