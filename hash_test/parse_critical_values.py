import sys
import csv

def parse(filepath: str) -> (list[float], map[int, list[float]]):
    probs = []
    table = dict()
    for i, line in enumerate(f.readlines()):
        line = line.strip()
        if not line:
            continue
        vals = line.split()
        if i == 0:
            probs = [float(s) for s in vals[1:]]
        else:
            v = int(vals[0].strip('.'))
            cs = [float(c) for c in vals[1:]]
            table[v] = cs
    return probs, table


def write_csv(probs: list[float], table: map[int, list[float]]) -> None:
    with open(sys.stdout, 'w') as f:
        wr = csv.writer(f)
        wr.writerow(['v', *probs])
        for v, cs in table.items():
            wr.writerow([v, *cs])


if __name__ == "__main__":
    probs, table = parse()
    write_csv(probs, table)


