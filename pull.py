import argparse
import subprocess

p = argparse.ArgumentParser()
p.add_argument('what', choices='comments submissions'.split())
p.add_argument('year', type=int)
p.add_argument('start_month', type=int, default=1)
p.add_argument('end_month', type=int, default=12)
args = p.parse_args()
what, y = args.what, args.year

for i in range(args.start_month, args.end_month+1):
    pre = 'RC' if what == 'comments' else 'RS'
    ext = '.bz2' if pre == 'RC' else '.zst'
    url = f'https://files.pushshift.io/reddit/{what}/{pre}_{y}-{i:02}{ext}'
    subprocess.run(['wget', url])
