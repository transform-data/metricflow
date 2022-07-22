import os
from typing import List, Optional
import yaml
from yamllint import config, linter, rules
from metricflow.model.parsing.dir_to_model import METRIC_TYPE, DATA_SOURCE_TYPE, MATERIALIZATION_TYPE

from metricflow.model.validations.validator_helpers import (
    FileContext,
    ModelValidationResults,
    ValidationError,
    ValidationIssue,
    ValidationWarning,
)

WARNING = "warning"
ERROR = "error"
DISABLE = "disable"
LEVEL = "level"


class ConfigLinter:  # noqa: D
    """A linter for checking the format of MetricFlow YAML model config files

    This config linter is a custom implementation of the linter provided by
    the yamllint package (https://pypi.org/project/yamllint/). For any problems
    that are found, ValidationIssues are created (with the appropriate level).
    Breaking issues will return errors. Issues that won't cause problems outside
    of maintainability will be warnings. A custom config can be provided on
    initialization. Please see the yamllint documentation for how that can be
    provided (https://yamllint.readthedocs.io/en/stable/configuration.html#custom-configuration-without-a-config-file)
    """

    DEFAULT_CONFIG = {
        "yaml-files": ["*.yaml", "*.yml"],
        "rules": {
            rules.braces.ID: DISABLE,
            rules.brackets.ID: DISABLE,
            rules.colons.ID: DISABLE,
            rules.commas.ID: DISABLE,
            rules.comments.ID: DISABLE,
            rules.comments_indentation.ID: DISABLE,  # TODO: Turn on as warning once warnings are less noisy in cli
            rules.document_start.ID: DISABLE,  # TODO: Turn on as warning once warnings are less noisy in cli
            rules.document_end.ID: DISABLE,
            rules.empty_lines.ID: DISABLE,
            rules.empty_values.ID: {LEVEL: ERROR},
            rules.hyphens.ID: DISABLE,
            rules.indentation.ID: DISABLE,  # TODO: Turn on as warning once warnings are less noisy in cli
            rules.key_duplicates.ID: {LEVEL: ERROR},
            rules.key_ordering.ID: DISABLE,
            rules.line_length.ID: DISABLE,
            rules.new_line_at_end_of_file.ID: DISABLE,
            rules.new_lines.ID: DISABLE,
            rules.octal_values.ID: DISABLE,  # TODO: Turn on as warning once warnings are less noisy in cli
            rules.quoted_strings.ID: DISABLE,
            rules.trailing_spaces.ID: DISABLE,  # TODO: Turn on as warning once warnings are less noisy in cli
            rules.truthy.ID: DISABLE,
        },
    }

    def __init__(self, lint_config: Optional[str] = None) -> None:  # noqa: D
        """Constructor for building a ConfigLinter to then use with MetricFlow YAML model config files

        Args:
            lint_config: A JSON-ified dict representing a yamllint config, see
                https://yamllint.readthedocs.io/en/stable/configuration.html#custom-configuration-without-a-config-file
                for details. The default config can be found as `ConfigLinter.DEFAULT_CONFIG`
        """
        self._lint_config = lint_config if lint_config else yaml.dump(data=self.DEFAULT_CONFIG)
        self._config = config.YamlLintConfig(self._lint_config)

    @staticmethod
    def add_additional_key_duplicates_info(problem: linter.LintProblem) -> None:
        """Add additional information to the key_duplicates problem description"""

        # desc is of the form f'duplication of key "{key}" in mapping'
        # the following split gives us ['duplication of key ', <key>, ' in mapping']
        key = problem.desc.split('"')[1]
        if key in [DATA_SOURCE_TYPE, MATERIALIZATION_TYPE, METRIC_TYPE]:
            problem.desc += f'. Key "{key}" is a top level object. If multiple top level objects are specified in the same file, they must be separated using "---"'

    def lint_file(self, file_path: str, file_name: str) -> List[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        with open(file_path) as f:
            for problem in linter.run(f, self._config):

                if problem.rule == rules.key_duplicates.ID:
                    self.add_additional_key_duplicates_info(problem=problem)

                if problem.level == ERROR:
                    issues.append(
                        ValidationError(
                            context=FileContext(file_name=file_name, line_number=problem.line),
                            message=problem.desc,  # type: ignore[misc]
                        )
                    )
                else:
                    issues.append(
                        ValidationWarning(
                            context=FileContext(file_name=file_name, line_number=problem.line),
                            message=problem.desc,  # type: ignore[misc]
                        )
                    )

        return issues

    def lint_dir(self, dir_path: str) -> ModelValidationResults:  # noqa: D
        issues: List[ValidationIssue] = []
        for root, _dirs, files in os.walk(dir_path):
            for file in files:
                if not (file.endswith(".yaml") or file.endswith(".yml")):
                    continue
                file_path = os.path.join(root, file)
                issues += self.lint_file(file_path, file)

        return ModelValidationResults.from_issues_sequence(issues)
