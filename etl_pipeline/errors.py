class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""


class InputError(Exception):
    """Raised when input data is invalid or unreadable."""


class LoadError(Exception):
    """Raised when loading data into PostgreSQL fails."""
