"""Safe scalar parsing including Bukkit's custom section tags."""

import yaml


class StorageLoader(yaml.SafeLoader):
    pass


def custom(loader, tag, node):
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    return loader.construct_scalar(node)


def mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ValueError("Duplicate YAML mapping key")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


StorageLoader.add_multi_constructor("!", custom)
StorageLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)


def parse(text):
    return yaml.load(text, Loader=StorageLoader)
