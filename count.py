import spacy
from spacy.tokens import DocBin
from tqdm import tqdm
import typer
from typing import List as lst

app = typer.Typer()
nlp = spacy.load('en_core_web_lg')

with open('stats.txt') as f:
    fs = (x.split() for x in f)
    fs = {x[0].split('.')[0].lower(): int(x[1]) for x in fs}

@app.command()
def check(files: lst[str]):
    ids = set()
    for f in tqdm(files, total=len(files)):
        bin = DocBin().from_disk(f)
        ids.update({x.user_data['id'] for x in bin.get_docs(nlp.vocab)})

    print(len(ids))
    assert(fs[f.split('/')[1].split('.')[0]] == len(ids))

@app.command()
def run(files: lst[str]):
    n = 0 
    for f in tqdm(files, total=len(files)):
        bin = DocBin().from_disk(f)
        n += len(bin) 

    print(n)

if __name__ == '__main__':
    app()
