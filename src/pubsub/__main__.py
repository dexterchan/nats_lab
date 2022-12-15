"""Console script for nats_lab."""
import argparse
import sys


def main():
    """Console script for nats_lab."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "server")
    parser.add_argument("-p", "port")
    args = parser.parse_args()

    server = args.server
    port = int(args.port)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
