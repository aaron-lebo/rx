import glob

import pandas as pd
from publicsuffix2 import get_public_suffix
from tqdm.auto import tqdm
import typer

app = typer.Typer()

def get_wiki(x):
    if x.find('?curid=') > -1:
        return ''

    y = x.split('/wiki/')
    if len(y) == 2:
        return y[1]
    return x.split('?title=')[1]

@app.command()
def main(dir: str):
    dfs, fs = [], glob.glob(f'{dir}/*.csv')
    for f in tqdm(fs, total=len(fs)):
        f1 = f'{dir}/match/urls/{f.split("/")[-1]}'
        df = pd.merge(pd.read_csv(f), pd.read_csv(f1), how='right', on='id')
        dfs.append(df)
    
    df = pd.concat(dfs)
    df['dom'] = df.url.str.extract('^([\w_-]+(\.[\w_-]+)+)')[0].str.lower()
    df = df[df.dom.notna()]
    df.dom = df.dom.map(get_public_suffix)
    df = df[df.dom.str.match('^.+\.[a-z]+$')]

    wiki = df.dom.str.match('^wikipedia\.(com|org)$')
    df.loc[wiki, 'wiki'] = df[wiki].url.map(get_wiki)

    df['dom url author subreddit wiki'.split()].to_csv('doms.csv')
    df.dom.value_counts().to_csv('doms1.csv')
    df.wiki.value_counts().to_csv('wiki.csv')

if __name__ == '__main__':
    app()
