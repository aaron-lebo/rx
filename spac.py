import argparse
from datetime import datetime
import sqlite3

import spacy
from spacy.tokens import DocBin

p = argparse.ArgumentParser()
p.add_argument('file', nargs='+')
args = p.parse_args()

cn = sqlite3.connect('rx.db')
cr = cn.cursor()

nlp = spacy.load('en_core_web_lg')

f = args.file[0] 
n = cr.execute(f'select count(*) from submissions').fetchone()[0]
qy = cr.execute(f'select id, created_utc, title, selftext from submissions order by id asc')

start = t = datetime.now()
utc = datetime(2001, 12, 31)
bin = DocBin(store_user_data=True) 
for i, x in enumerate(qy.fetchall()):
    date = utc.date()
    utc1 = datetime.fromtimestamp(x[1])
    doc = nlp('\n'.join(x[2:4]))
    doc.user_data = dict(id=x[0])
    bin.add(doc)

    end = n == i + 1
    t1 = datetime.now()
    if end or (t1-t).total_seconds() > 1:
        print(f'1 1  {f}  {n:12,}  {utc}  {(i+1)/n*100:6.2f}%  {t1-start}', end='\r')
        t = t1
    if end or date.year != 2001 and date != utc1.date():
        bin.to_disk(f'rs_{date}.spacy')
        bin = DocBin(store_user_data=True) 

    utc = utc1
