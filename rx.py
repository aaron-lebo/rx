import argparse
import collections
import json
import lzma
import sys

parser = argparse.ArgumentParser()
parser.add_argument('command', choices=['count-subs', 'sample'])
parser.add_argument('files', nargs='*')
args = parser.parse_args()

total = 0#int(sys.argv[2]) if len(sys.argv) > 2 else 0
count, things = collections.Counter(),  []
for file in args.files:
    with lzma.open(file) as f:
        try:
            for n, line in enumerate(f):
                thing = json.loads(line)
                count[thing['subreddit']] += 1
                things.append(thing)
                if total and n + 1 == total:
                    break
        except EOFError:
            pass

if args.command == 'count-subs':
    for item in count.most_common():
        print(item)
else:
    json.dump(things, sys.stdout, indent=2)
