import argparse
import sys

from .errors import ConfigError, InputError, LoadError
from .pipeline import run


def parse_args():
    parser = argparse.ArgumentParser(
        description="Configurable JSON to PostgreSQL ETL Pipeline"
    )
    parser.add_argument("--input", required=True, help="Path to input JSON file.")
    parser.add_argument(
        "--app-config", required=True, help="Path to application config file."
    )
    parser.add_argument(
        "--mapping-config", required=True, help="Path to mapping config file."
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        run(
            input_path=args.input,
            app_config=args.app_config,
            mapping_config=args.mapping_config,
        )
    except (ConfigError, InputError, LoadError) as exc:
        print(f"Pipeline error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
