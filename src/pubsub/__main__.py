"""Console script for nats_lab."""
import argparse
import sys


def main():
    """Console script for nats_lab."""
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "nats_lab.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
