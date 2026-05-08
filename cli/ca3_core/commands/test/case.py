from dataclasses import dataclass
from pathlib import Path

import yaml

from ca3_core.ui import UI

TESTS_FOLDER = "tests/"


@dataclass
class TestCase:
    """A single test case loaded from a YAML file."""

    name: str
    prompt: str
    file_path: Path
    sql: str | None = None
    expected_sql_contains: list[str] | None = None
    forbidden_sql_contains: list[str] | None = None
    expected_columns: list[str] | None = None
    expected_rows: list[dict] | None = None
    threshold: float | None = None

    @classmethod
    def from_yaml(cls, file_path: Path) -> list["TestCase"]:
        """Load a test case from a YAML file."""
        with open(file_path) as f:
            data = yaml.safe_load(f)

        cases = data if isinstance(data, list) else [data]
        test_cases: list[TestCase] = []
        for index, item in enumerate(cases, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"test case #{index} must be a mapping")

            prompt = item.get("prompt") or item.get("question")
            if not prompt:
                raise ValueError(f"test case #{index} must define prompt or question")

            test_cases.append(
                cls(
                    name=item.get("name") or item.get("id") or f"{file_path.stem}_{index}",
                    prompt=prompt,
                    sql=item.get("sql"),
                    expected_sql_contains=item.get("expected_sql_contains"),
                    forbidden_sql_contains=item.get("forbidden_sql_contains"),
                    expected_columns=item.get("expected_columns"),
                    expected_rows=item.get("expected_rows"),
                    threshold=item.get("threshold"),
                    file_path=file_path,
                )
            )

        return test_cases


def discover_tests(project_path: Path) -> list[TestCase]:
    """Discover all test cases in the tests/ folder."""
    tests_dir = project_path / TESTS_FOLDER

    if not tests_dir.exists():
        UI.warn(f"Tests folder not found: {tests_dir}")
        return []

    test_files = [
        *[path for path in tests_dir.glob("*.yml") if not path.name.startswith(".")],
        *[path for path in tests_dir.glob("*.yaml") if not path.name.startswith(".")],
    ]

    if not test_files:
        UI.warn(f"No test files found in {tests_dir}")
        return []

    test_cases = []
    for file_path in sorted(test_files):
        try:
            test_cases.extend(TestCase.from_yaml(file_path))
        except Exception as e:
            UI.error(f"Failed to load {file_path.name}: {e}")

    return test_cases
