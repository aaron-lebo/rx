import glob
import os

import pandas as pd
from publicsuffix2 import get_public_suffix
from tqdm.auto import tqdm
import typer

app = typer.Typer()

def split(x):
    return os.path.normpath(x).split('/')[-1]

def get_wiki(x):
    if x.find('?curid=') > -1:
        return ''

    y = x.split('/wiki/')
    if len(y) == 2:
        return y[1]
    return x.split('?title=')[-1]

@app.command()
def main(dir: str):
    dirs = sorted(f.path for f in os.scandir(dir) if f.is_dir())
    dirs1 = [] 
    for dir in dirs:
        f = split(dir)
        if f[:3] in ('rc_', 'rs_') and not os.path.exists(f'out/dom/{f}.csv'):
            dirs1.append(dir) 

    os.makedirs(f'out/dom', exist_ok=True)
    for i, dir in enumerate(dirs1):
        f = split(dir)
        x = sorted(glob.glob(f'{dir}/*.csv'))
        y = sorted(glob.glob(f'{dir}/match/urls/*.csv'))
        t = tqdm(zip(x, y), total=len(x))
        dfs = []
        for x, y in t:
            t.set_description(f'{i+1:2}/{len(dirs1):2} {f}')
            df = pd.merge(pd.read_csv(x), pd.read_csv(y), how='right', on='id')
            dfs.append(df)

        df = pd.concat(dfs)
        df['url'] = df['url_y'] if 'url_y' in df.columns else df['url']
        df['dom'] = df.url.str.extract('^([\w_-]+(\.[\w_-]+)+)')[0].str.lower()
        df = df[df.dom.notna()]
        df.dom = df.dom.map(get_public_suffix)
        df = df[df.dom.str.match('^.+\.[a-z]+$')]

        wiki = df.dom.str.match('^wikipedia\.(com|org)$')
        df.loc[wiki, 'wiki'] = df[wiki].url.map(get_wiki)

        df['dom url id author subreddit wiki'.split()].to_csv(f'out/dom/{f}.csv', index=False)

if __name__ == '__main__':
    app()
