from .errors import ConfigError


def get_collection_mapping(mapping_config: dict, collection_name: str) -> dict:
    collections = mapping_config.get("collections")
    if collections is None:
        raise ConfigError("Mapping config is missing 'collections'.")

    return collections.get(collection_name)
