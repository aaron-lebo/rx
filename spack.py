import json 

import spacy
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer

nlp = spacy.load('en_core_web_lg')

ks = 'id,created_utc,edited,retrieved_on,author,subreddit,url'.split(',')

def main(file: str):
    with open(file) as f:
        n = sum(1 for x in f) 
        f.seek(0)
        bin = DocBin()
        for i, x in enumerate(tqdm(f, total=n)):
            x = json.loads(x)
            d = nlp('\n'.join([x['title'], x['selftext']]))
            d.user_data = {k: x.get(k) for k in ks}
            bin.add(d)
            if not i % 1000:
                bin.to_disk(f'{file}.spacy')

    bin.to_disk(f'{file}.spacy')

if __name__ == '__main__':
    typer.run(main)
