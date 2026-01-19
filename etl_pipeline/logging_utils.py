import logging


def configure_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger("etl_pipeline")
