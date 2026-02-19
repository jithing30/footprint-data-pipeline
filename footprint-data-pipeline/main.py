import sys
import src.ingest_footprint as ingest


def main(**kwargs):
    ingest.ingest_data(**kwargs)


if __name__ == "__main__":
    # Get year from command line if provided
    # Example: python main.py 2025

    if len(sys.argv) > 1:
        year = int(sys.argv[1])
    else:
        year = None

    print("in main")

    main(year=year)
