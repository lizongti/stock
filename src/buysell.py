import os

higheast = "higheast"
loweast = "loweast"


def read_txt(name: str):
    txt_path = os.path.join(os.path.dirname(os.getcwd()), "data", "txt",
                            "%s.txt" % (name))
    rows = []
    with open(txt_path) as f:
        lines = f.read().splitlines()
        for line in lines:
            rows.append(line.split("\t"))

    print(rows)


def main():
    read_txt(higheast)


if __name__ == "__main__":
    main()

    
