import os


def write_txt(name: str, rows: list[str]):
    txt_path = os.path.join(os.path.dirname(os.getcwd()), 'data', 'txt',
                            '%s.txt' % (name))
    rows = []
    with open(txt_path, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            rows.append(line.split('\t'))


def read_txt(name: str):
    txt_path = os.path.join(os.path.dirname(os.getcwd()), 'data', 'txt',
                            '%s.txt' % (name))
    rows = []
    with open(txt_path, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            rows.append(line.split('\t'))
    return rows


def write_csv(name: str, rows: list[str]):
    csv_path = os.path.join(os.path.dirname(os.getcwd()), 'data', 'csv',
                            '%s.csv' % (name))
    with open(csv_path, 'w') as f:
        for row in rows:
            f.write(','.join(row)+'\n')


def __convert():
    rootdir = os.path.join(os.path.dirname(os.getcwd()), 'data', 'txt')
    list = os.listdir(rootdir)
    for name in list:
        name_no_suffix = os.path.splitext(name)[0]
        write_csv(name_no_suffix, read_txt(name_no_suffix))


if __name__ == '__main__':
    __convert()
