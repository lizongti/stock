
import minute
import traceback


def main():
    minute.update()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(repr(e))
        print(traceback.format_exc())
