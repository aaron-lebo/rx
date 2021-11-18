import collections
import glob
import random

from fastapi import FastAPI, Form, Request
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import spacy
from spacy.tokens import DocBin

app = FastAPI()

nlp = spacy.load('en_core_web_lg')
bin = DocBin(store_user_data=True)
try:
    bin = bin.from_disk('train.spacy')
except FileNotFoundError:
    pass

sam = {}
sbrs = collections.defaultdict(collections.Counter)

@app.on_event('startup')
async def load():
    global sam, sbrs
    done = set()
    for x in bin.get_docs(nlp.vocab):
        sbrs[x.user_data['subreddit']].update(x.cats)
        done.add(x.user_data['id'])

    docs = set() 
    fs = glob.glob('out/match/*.csv')
    random.shuffle(fs)
    for f in fs:
        docs.update({x['id'] for _, x in pd.read_csv(f).iterrows() if x['id'] not in done})

    sam = {x: None for x in random.sample(docs, 200)}
    for f in fs:
        bin1 = DocBin().from_disk(f'out/{f[10:-4]}.spacy')
        for x in bin1.get_docs(nlp.vocab): 
            id = x.user_data['id']
            if id in sam:
               sam[id] = x
     
ts = Jinja2Templates(directory='templates')

def render(f: str, r: Request, **kws):
    return ts.TemplateResponse(f, dict(request=r, **kws))

@app.get('/', response_class=HTMLResponse)
async def cat(r: Request):
    x = sam and list(sam.values())[0]
    return render('cat.html', r, text=str(x), x=x.user_data, remaining=len(sam))

@app.post('/cat/s/{id}')
async def cat_(id: str, cat: str=Form(...)):
    if cat in ('yes', 'no', 'skip'):
        x = sam[id]
        x.cats = dict(yes=int(cat=='yes'), no=int(cat=='no'), skip=int(cat=='skip'))
        bin.add(x)
        bin.to_disk('train.spacy')
        sbrs[x.user_data['subreddit']].update([cat])
        del sam[id]
    return RedirectResponse('/', status_code=303)

@app.get('/subreddit', response_class=HTMLResponse)
async def subreddit(r: Request):
    return render('subreddit.html', r, xs=sbrs)
