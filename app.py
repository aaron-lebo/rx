from collections import Counter, defaultdict, OrderedDict
from datetime import datetime
import re
import sqlite3

from flask import Flask, g, render_template, request
import jinja2

app = Flask(__name__)

things = OrderedDict()
dates = defaultdict(set)
dates_cnt = Counter()
subreddits = defaultdict(set)
subreddits_cnt = Counter()

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect('rx.db')
    db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def append(submission, comment=None):
    obj = submission = dict(submission)
    if comment:
        obj = obj.copy()
        obj.update(dict(comment))

    id = obj['id']
    created = datetime.utcfromtimestamp(obj['created_utc'])
    subreddit = obj['subreddit']

    obj = dict(obj, created=created, url_text=obj['url'])
    obj['reddit_url'] = '/'.join([
        'http://reddit.com/r',
        subreddit,
        'comments',
        submission['id'].replace('t3_', ''),
        f'x/{id.replace("t1_", "")}' if comment else ''
    ])
    for match in obj['matches'].split(', '):
        for k in ('body',) if comment else ('title', 'url_text', 'selftext'):
            m = re.search(f'\\b({match})\\b' , obj[k], re.IGNORECASE)
            if m:
                start, end = m.span()
                span = jinja2.Markup(f'<mark>{m.group()}</mark>')
                obj[k] = obj[k][:start] + span + obj[k][end:]
                break

    dates[created.strftime('%b %Y')].add(id)
    subreddits[subreddit].add(id)
    things[id] = obj

with app.app_context():
    cur = get_db().execute('select * from submissions order by id')
    submissions = cur.fetchall()
    cur.close()
    for s in submissions:
        if s[-1]:
            append(s)
            continue

        cur = get_db().execute("""
            select * from comments where parent_id = ? and matches != ''
            union
            select * from comments where link_id = ? and matches != ''
            order by id
        """, (s[0], s[0]))
        comments = cur.fetchall()
        cur.close()
        for c in comments:
            append(s, c)

    total = len(things)
    for k, v in dates.items():
        dates_cnt[k] = len(v)
    for k, v in subreddits.items():
        subreddits_cnt[k] = len(v)

@app.route('/')
def index():
    date = request.args.get('date')
    subreddit = request.args.get('subreddit')
    page = int(request.args.get('page', 1))
    d_ids = dates.get(date, set())
    s_ids = subreddits.get(subreddit, set())
    if d_ids and s_ids:
        ids = d_ids & s_ids
    else:
        ids = d_ids or s_ids
    things1 = [v for k, v in things.items() if k in ids] if date or subreddit else list(things.values())
    n = len(ids or things1)
    end = page * 500
    start = end - 500
    return render_template(
        'index.html',
        date = date,
        subreddit = subreddit,
        start = start,
        page = page,
        end = end if end < n else n,
        n = n,
        things = things1[start:end],
        total = total,
        dates = dates_cnt.most_common(),
        subreddits = subreddits_cnt.most_common()
    )
