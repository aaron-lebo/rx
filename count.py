import spacy
from spacy.tokens import DocBin
import typer
from typing import List

nlp = spacy.load('en_core_web_lg')

def main(files: List[str]):
    n = 0 
    for i, f in enumerate(files):
        bin = DocBin().from_disk(f)
        print(f'{i:03} {f} {len(bin)}')
        n += len(bin)

    print(i, n) 

if __name__ == '__main__':
    typer.run(main)
