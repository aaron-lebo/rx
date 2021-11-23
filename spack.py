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

def main(file: str):
    file1 = file.split('/')[-1].lower()
    pre = file1[:3]
    assert(pre in ('rc_', 'rs_'))
    os.makedirs(f'out/{file1}')
    with open(file) as f:
        n = sum(1 for x in f) 
        f.seek(0)

        n_proc = os.cpu_count()
        n_proc = 1 if n_proc == 1 else n_proc-1
        p = nlp.pipe((load(x) for x in f), as_tuples=True, n_process=n_proc)
        bin = DocBin(store_user_data=True)
        for i, (d, x) in enumerate(tqdm(p, total=n)):
            d.user_data = {k: x.get(k) for k in ks}
            for k in ks_int:
                d.user_data[k] = int(x.get(k, 0))

            if i and not i % 100000:
                f = f'{out/{file1}/x["id"]}.spacy'    
                if os.path.isfile(f):
                    raise FileExistsError

                bin.to_disk(f)
                bin = DocBin(store_user_data=True)

            bin.add(d)

        bin.to_disk(f)

if __name__ == '__main__':
    typer.run(main)
