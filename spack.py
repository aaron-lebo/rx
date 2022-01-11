import os
import json 

import pandas as pd
import spacy
from spacy.tokens import DocBin
from tqdm.auto import tqdm
import typer
from typer import Option

nlp = spacy.load('en_core_web_lg')

ks_ = 'id created_utc edited retrieved_on author subreddit'.split()
ks_sub = ['url']
ks_com = 'link_id parent_id'.split()

def load(s: str, pre: str):
    x = json.loads(s)
    if pre == 'rs_':
        return '\n'.join([x['title'], x['selftext']]), x
    return x['body'], x

fs = pd.read_csv('stats.csv')
fs.file = fs.file.str.lower()
fs = dict(fs.values)

def main(file: str, start: int = Option(0), procs: int = Option(os.cpu_count()), full: bool = Option(True)):
    file1 = file.split('/')[-1].lower()
    pre = file1[:3]
    ks = ks_+(ks_sub if pre == 'rs_' else ks_com)
    assert(pre in ('rc_', 'rs_'))
    with open(file) as f:
        n = sum(1 for x in f)
        f.seek(0)
        assert(n == fs[file1])
        os.makedirs(f'out/{file1}', exist_ok=True)
        if start:
            n -= start
            for i, _ in enumerate(f):
                if i == start-1:
                    break

        p = (load(x, pre) for x in f)
        p = nlp.pipe(p, as_tuples=True, n_process=procs) if full else p
        bin = DocBin(store_user_data=True)
        dat = []
        f = ''    
        for i, (d, x) in enumerate(tqdm(p, total=n, smoothing=0.1)):
            dat.append([x.get(k) for k in ks])
            if not full:
                continue

            f = f'out/{file1}/{x["id"]}.spacy'    
            d.user_data['id'] = x['id']
            bin.add(d)
            if i and not i % 20000:
                if os.path.isfile(f):
                    raise FileExistsError

                bin.to_disk(f)
                bin = DocBin(store_user_data=True)

        if os.path.isfile(f):
            raise FileExistsError
        if full:
            bin.to_disk(f)

        df = pd.DataFrame(dat, columns=ks)
        df.to_csv(f'{file1}.csv')
        assert(n == i+1)

if __name__ == '__main__':
    typer.run(main)
