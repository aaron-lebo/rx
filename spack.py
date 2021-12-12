import os

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

with open('statistics.csv') as f:
    next(f)
    fs = (x.split() for x in f)
    fs = {x[0].split('.')[0].lower(): int(x[2]) for x in fs}

def main(file: str):
    file1 = file.split('/')[-1].lower()
    pre = file1[:3]
    assert(pre in ('rc_', 'rs_'))
    with open(file) as f:
        n = sum(1 for x in f) 
        f.seek(0)
        assert(fs[file1] == n)
        os.makedirs(f'out/{file1}', exist_ok=True)

        n_proc = os.cpu_count()
        n_proc = 1 if n_proc == 1 else n_proc-1
        p = nlp.pipe((load(x) for x in f), as_tuples=True, n_process=n_proc)
        bin = DocBin(store_user_data=True)
        for i, (d, x) in enumerate(tqdm(p, total=n)):
            d.user_data = {k: x.get(k) for k in ks}
            for k in ks_int:
                d.user_data[k] = int(x.get(k, 0))

            f = f'out/{file1}/{x["id"]}.spacy'    
            bin.add(d)
            if i and not i % 20000:
                if os.path.isfile(f):
                    raise FileExistsError

                bin.to_disk(f)
                bin = DocBin(store_user_data=True)

        if os.path.isfile(f):
            raise FileExistsError

        bin.to_disk(f)
        assert(fs[file1] == i+1)

if __name__ == '__main__':
    typer.run(main)
