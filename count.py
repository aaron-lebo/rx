import os

import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer
from typing import List

nlp = spacy.load('en_core_web_lg')

def main(files: List[str]):
    results =  []
    for i, f in enumerate(files):
        bin = DocBin().from_disk(f)
        print(i, f, len(bin))

    df = pd.DataFrame(results, columns=['id', 'matches', 'combos'])
    df = df.set_index('id')
    df.to_csv(f)

if __name__ == '__main__':
    typer.run(main)
