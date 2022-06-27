import os
import glob

import pandas as pd
from tqdm.auto import tqdm
import typer

app = typer.Typer()

def split(file: str):
    f = os.path.normpath(file.lower()).split(os.sep)[-1]
    assert(f[:3] in ('rc_', 'rs_'))
    return f[:10]

def add(cnts, k, df):
    v = cnts.get(k)
    if v is None:
        cnts[k] = df.value_counts(list(df.columns))
    else:
        cnts[k] = v.add(df.value_counts(list(df.columns)), fill_value=0)

@app.command()
def count(path: str):
    cnts, fs = {}, sorted(glob.glob(path))
    for f in tqdm(fs, total=len(fs)):
        #k, df = split(f), pd.read_parquet(f, columns=['author', 'subreddit'])
        k, df = split(f), pd.read_parquet(f, columns=['subreddit'])
        add(cnts, k, df)

    dfs = []
    for k, v in cnts.items():
        df = v.to_frame('n')
        df['k'] = k
        dfs.append(df)

    df = pd.concat(dfs)
    df.n = df.n.astype(int)
    df.to_csv('cnts.csv')

@app.command()
def matches(dir: str):
    rs, fs = set(), sorted(glob.glob('terms/rs_*.pq'))
    for f in tqdm(fs, total=len(fs)):
        rs.update(pd.read_parquet(f, columns=[]).index)

    rs = pd.DataFrame(rs, columns=['id']).to_csv('rs.csv', index=False)

    rc, fs = set(), sorted(glob.glob('terms/rc_*.pq'))
    for f in tqdm(fs, total=len(fs)):
        rc.update(pd.read_parquet(f, columns=[]).index)

    pd.DataFrame(rc, columns=['id']).to_csv('rc.csv', index=False)

    dfs, rs_rel, rc_rel = [], set(), {'t1_'+x for x in rc}
    fs = sorted(glob.glob(f'{dir}/rc_*.pq'))
    for f in tqdm(fs, total=len(fs)):
        dfs.append(pd.read_parquet(f, columns=['link_id', 'parent_id']))
        if len(dfs) > 7:
            df = dfs = pd.concat(dfs)
            rc_rel.update({'t1_'+x for x in df[df.parent_id.isin(rc_rel)].index})
            rs_rel.update(df[df.index.isin(rc)].link_id)
            dfs = df = []

    if dfs:
        df = dfs = pd.concat(dfs)
        rc_rel.update({'t1_'+x for x in df[df.parent_id.isin(rc_rel)].index})
        rs_rel.update(df[df.index.isin(rc)].link_id)

    pd.DataFrame(rc_rel, columns=['id']).to_csv('rc_rel.csv', index=False)
    pd.DataFrame(rs_rel, columns=['id']).to_csv('rs_rel.csv', index=False)

@app.command()
def count_matches(dir: str, comments: bool, relatives: bool):
    pre = 'rc' if comments else 'rs'
    ids = matches = set(pd.read_csv(pre+'.csv').id)
    if relatives:
        rels = ids = set(pd.read_csv(pre+'_rel.csv').id.str[3:])
        rels.difference_update(matches)
        matches = []

    cnts, fs = {}, sorted(glob.glob(f'{dir}/{pre}_*.pq'))
    for f in tqdm(fs, total=len(fs)):
        k, df = split(f), pd.read_parquet(f, columns=['author', 'subreddit'])
        add(cnts, k, df[df.index.isin(ids)])

    dfs = []
    for k, v in cnts.items():
        df = v.to_frame('n')
        df['k'] = k
        dfs.append(df)

    df = pd.concat(dfs)
    df.n = df.n.astype(int)
    df.to_csv(f'{pre}{"_rel" if relatives else ""}_cnts.csv')

if __name__ == '__main__':
    app()
