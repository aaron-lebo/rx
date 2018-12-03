import json
import lzma
import sys

total = int(sys.argv[2]) if len(sys.argv) > 2 else 0
with lzma.open(sys.argv[1]) as f:
    try:
        for n, line in enumerate(f):
            json.dump(json.loads(line), sys.stdout, indent=2)
            if total and n + 1 == total:
                break
    except EOFError:
        pass
