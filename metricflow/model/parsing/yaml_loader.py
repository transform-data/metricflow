from __future__ import annotations

from io import StringIO
from typing import Dict, Iterator

import yaml

"""This is the name of the field the SafeLineLoaderWithAddedContext adds to the parsed yaml
   we retrieve line number and the file name from the context stored at this key"""
PARSING_CONTEXT_KEY = "__parsing_context__"


class ParsingContext:
    """Container class for file slice information used to populate model metadata for certain objects"""

    def __init__(self, start_line: int, end_line: int, filename: str, content_node: yaml.Node) -> None:
        """Initializer for the ParsingContext class.

        The contents are represented internally as a yaml.Node in order to allow for lazy serialization to
        string representations.
        """
        self.start_line = start_line
        self.end_line = end_line
        self.filename = filename
        self._content_node = content_node

    @property
    def content(self) -> str:
        """Serialized contents associated with the file slice represented by this ParsingContext object

        This should only be called when a string representation of the contents are needed.
        """
        return yaml.serialize(node=self._content_node)

    def __str__(self) -> str:  # noqa: D
        return f"line: {self.start_line}, filename: {self.filename}"


class YamlConfigLoader:
    """Helper class for loading YAML config strings into an iterator of YAML output"""

    @staticmethod
    def load_all_with_context(name: str, contents: str) -> Iterator:
        """Wraps the yaml.load_all method and returns the resulting iterator with parsing context added to output

        This replaces any calls to yaml.load_all(loader=SafeLineLoaderWithAddedContext), which internally adds
        ParsingContext info. Note PyYAML reads the name property from the input stream IF that input stream is a file
        object, otherwise it replaces it with a constant. Therefore, we use as StringIO instance to pass the contents
        into PyYAML and set the value of the name property on the file object to the name parameter here.
        """
        with StringIO(initial_value=contents) as stream:
            stream.name = name
            for document in yaml.load_all(stream=stream, Loader=SafeLineLoaderWithAddedContext):
                yield document

    @staticmethod
    def is_valid_yaml_file_ending(filename: str) -> bool:
        """Checks if YAML file name ends with one of the supported suffixes"""
        return filename.endswith(".yaml") or filename.endswith(".yml")


class SafeLineLoaderWithAddedContext(yaml.SafeLoader):
    """Adds special field __parsing_context__ to all mappings.

    Credit: https://stackoverflow.com/questions/13319067/parsing-yaml-return-with-line-number
    """

    # we may also want to consider this parser https://yaml.readthedocs.io/en/latest/
    # which supports yaml1.2 and maintains round-trip parsing, for now we use
    # the more established road
    # https://stackoverflow.com/questions/55441300/how-can-i-get-the-parent-node-within-yaml-loader-add-contructor
    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> Dict:
        """Override of the construct_mapping method in the PyYAML SafeLoader class

        This override exists in order to populate the parsing context object with file location
        and raw YAML content information, which will be used to populate the Metadata model construct
        for nodes which request it. The file identifier (node.start_mark.name) is derived from the
        name property on the file-like object passed in as the stream parameter of the load_all call.
        The line numbers are part of the start and end mark properties.

        The content_node is a PyYAML node. We do not serialize it inside the loader because it's a full
        serialization pass on whatever contents this particular mapping node might contain, which could
        be the entire model collection. Rather, we store the node and serialize it on-demand later.

        Note: PyYAML uses metaclasses quite heavily so construct_mapping is in fact defined in the
        SafeConstructor class.
        """
        mapping = super(SafeLineLoaderWithAddedContext, self).construct_mapping(node, deep=deep)
        mapping[PARSING_CONTEXT_KEY] = ParsingContext(
            node.start_mark.line + 1, node.end_mark.line, node.start_mark.name, node
        )  # change to 1-indexed

        return mapping
