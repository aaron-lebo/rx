from datetime import datetime

import orjson 
import spacy
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer

nlp = spacy.load('en_core_web_lg')

ks = 'id author subreddit url'.split()
ks_int = 'created_utc edited retrieved_on'.split()

def load(s: str):
    x = orjson.loads(s)
    return '\n'.join([x['title'], x['selftext']]), x

def main(file: str):
    f = open(file)
    n = sum(1 for x in f) 
    f.seek(0)

    xs = (load(x) for x in f)
    p = nlp.pipe(xs, as_tuples=1, n_process=3)
    bin = DocBin(store_user_data=True)
    utc = None
    for i, (d, x) in enumerate(tqdm(p, total=n)):
        d.user_data = {k: x.get(k) for k in ks}
        for k in ks_int:
            d.user_data[k] = int(x.get(k, 0))

        bin.add(d)

        utc1 = utc
        utc = datetime.fromtimestamp(d.user_data['created_utc']).date()
        if utc1 and utc != utc1:
            bin.to_disk(f'{utc}.spacy')
            bin = DocBin(store_user_data=True)

    bin.to_disk(f'{utc}.spacy')
    f.close()

if __name__ == '__main__':
    typer.run(main)
