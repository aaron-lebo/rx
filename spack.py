import os

import orjson 
import spacy
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer

nlp = spacy.load('en_core_web_lg')

ks_sub = 'id author link_id parent_id'.split()
ks_com = 'id author subreddit url'.split()
ks_int = 'created_utc edited retrieved_on'.split()

def load(s: str, pre: str):
    x = orjson.loads(s)
    if pre == 'rs_':
        return '\n'.join([x['title'], x['selftext']]), x
    return x['body'], x

with open('stats.txt') as f:
    fs = (x.split() for x in f)
    fs = {k.split('.')[0].lower(): int(v) for k, v in fs}

def main(file: str, start: int = typer.Option(0), procs: int = typer.Option(os.cpu_count())):
    file1 = file.split('/')[-1].lower()
    pre = file1[:3]
    ks = ks_sub if pre == 'rs_' else ks_com
    assert(pre in ('rc_', 'rs_'))
    with open(file) as f:
        n = sum(1 for x in f)
        f.seek(0)
        assert(fs[file1] == n)
        os.makedirs(f'out1/{file1}', exist_ok=True)
        if start:
            n -= start
            for i, _ in enumerate(f):
                if i == start-1:
                    break

        p = nlp.pipe((load(x, pre) for x in f), as_tuples=True, n_process=procs)
        bin = DocBin(store_user_data=True)
        for i, (d, x) in enumerate(tqdm(p, total=n, smoothing=0.1)):
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
        assert(n == i+1)

if __name__ == '__main__':
    typer.run(main)
