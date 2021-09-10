import os

higheast = 'higheast'
loweast = 'loweast'


def read_txt(name: str):
    txt_path = os.path.join(os.path.dirname(os.getcwd()), 'data', 'txt',
                            '%s.txt' % (name))
    rows = []
    with open(txt_path) as f:
        lines = f.read().splitlines()
        for line in lines:
            rows.append(line.split('\t'))
    return rows


def main():
    higeast_rows = read_txt(higheast)
    loweast_rows = read_txt(loweast)
    stats = {}
    for values in higeast_rows:
        if values[1] not in stats:
            stats[values[1]] = 0
        else:
            stats[values[1]] += int(values[0])
    for values in loweast_rows:
        if values[1] not in stats:
            stats[values[1]] = 0
        else:
            stats[values[1]] -= int(values[0])

    result = {k: v for k, v in sorted(stats.items(), key=lambda item: item[1])}


if __name__ == '__main__':
    main()
