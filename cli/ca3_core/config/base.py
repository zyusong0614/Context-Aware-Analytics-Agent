from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator
from rich.console import Console

if TYPE_CHECKING:
    from ibis import BaseBackend

from ca3_core.ui import UI, ask_confirm, ask_select

from .databases import DATABASE_CONFIG_CLASSES, AnyDatabaseConfig, DatabaseTemplate, DatabaseType, parse_database_config
from .error_handler import format_all_validation_errors
from .llm import LLMConfig


class Ca3ConfigError(Exception):
    """Raised when ca3 config loading fails."""

    pass


class Ca3Config(BaseModel):
    """ca3 project configuration."""

    project_name: str = Field(description="The name of the ca3 project")
    databases: list[AnyDatabaseConfig] = Field(default_factory=list, description="The databases to use")
    llm: LLMConfig | None = Field(default=None, description="The LLM configuration")

    _missing_env_vars: dict[str, None] = {}

    @model_validator(mode="before")
    @classmethod
    def parse_databases(cls, data: dict) -> dict:
        """Parse database configs into their specific types."""
        if "databases" in data and isinstance(data["databases"], list):
            data["databases"] = [parse_database_config(db) if isinstance(db, dict) else db for db in data["databases"]]
        return data

    @classmethod
    def promptConfig(cls, project_name: str, existing: "Ca3Config | None" = None) -> "Ca3Config":
        """Interactively prompt the user for all ca3 configuration options.

        If existing config is provided, shows current items and allows adding more.
        """
        if existing:
            return cls._prompt_extend(existing)

        databases = cls._prompt_databases()
        enable_profiling = cls._prompt_enable_profiling(databases)
        databases = cls._configure_profiling_templates(databases, enable_profiling)
        llm, enable_ai_summary = cls._prompt_llm(databases=databases)
        databases = cls._configure_ai_summary_templates(databases, llm, enable_ai_summary)

        return cls(
            project_name=project_name,
            databases=databases,
            llm=llm,
        )

    @classmethod
    def _prompt_extend(cls, existing: "Ca3Config") -> "Ca3Config":
        """Extend an existing config by adding more items."""
        databases = list(existing.databases)
        llm = existing.llm

        # Show current config summary
        UI.title("Current Configuration")
        if databases:
            UI.print(f"  Databases: {', '.join(db.name for db in databases)}")
        if repos:
            UI.print(f"  Repos: {', '.join(r.name for r in repos)}")
        if llm:
            UI.print(f"  LLM: {llm.provider}")
        if slack:
            UI.print("  Slack: configured")
        if notion:
            UI.print("  Notion: configured")
        if mcp:
            UI.print("  MCP: configured")
        if skills:
            UI.print("  Skills: configured")
        UI.print()

        # Prompt for additions
        new_databases = cls._prompt_databases(has_existing=bool(existing.databases))
        if new_databases:
            enable_profiling = cls._prompt_enable_profiling(new_databases)
            new_databases = cls._configure_profiling_templates(new_databases, enable_profiling)
        databases.extend(new_databases)
        repos.extend(cls._prompt_repos(has_existing=bool(existing.repos)))

        if llm:
            enable_ai_summary = cls._prompt_enable_ai_summary_templates(databases)
        else:
            llm, enable_ai_summary = cls._prompt_llm(databases=databases)

        if not slack:
            slack = cls._prompt_slack()

        if not notion:
            notion = cls._prompt_notion()

        if not mcp:
            mcp = cls._prompt_mcp(existing.project_name)

        if not skills:
            skills = cls._prompt_skills(existing.project_name)

        databases = cls._configure_ai_summary_templates(databases, llm, enable_ai_summary)

        return cls(
            project_name=existing.project_name,
            databases=databases,
            llm=llm,
        )

    @staticmethod
    def _prompt_databases(has_existing: bool = False) -> list[AnyDatabaseConfig]:
        """Prompt for database configurations using questionary."""
        databases: list[AnyDatabaseConfig] = []

        prompt = "Add more database connections?" if has_existing else "Set up database connections?"
        if not ask_confirm(prompt, default=not has_existing):
            return databases

        while True:
            UI.title("Database Configuration")

            db_type = ask_select("Select database type:", choices=DatabaseType.choices())

            config_class = cast(Any, DATABASE_CONFIG_CLASSES[DatabaseType(db_type)])
            db_config = cast(AnyDatabaseConfig, config_class.promptConfig())
            databases.append(db_config)

            UI.success(f"Added database: {db_config.name}")

            if not ask_confirm("Add another database?", default=False):
                break

        return databases

    @staticmethod
    def _prompt_repos(has_existing: bool = False) -> list[RepoConfig]:
        """Prompt for repository configurations using questionary."""
        repos: list[RepoConfig] = []

        prompt = "Add more git repositories?" if has_existing else "Set up git repositories?"
        if not ask_confirm(prompt, default=not has_existing):
            return repos

        while True:
            repo_config = RepoConfig.promptConfig()
            repos.append(repo_config)
            UI.success(f"Added repository: {repo_config.name}")

            if not ask_confirm("Add another repository?", default=False):
                break

        return repos

    @staticmethod
    def _prompt_llm(databases: list[AnyDatabaseConfig] | None = None) -> tuple[LLMConfig | None, bool]:
        """Prompt for LLM configuration and optional ai_summary settings."""
        if ask_confirm("Set up LLM configuration?", default=True):
            enable_ai_summary = Ca3Config._prompt_enable_ai_summary_templates(databases or [])
            return LLMConfig.promptConfig(prompt_annotation_model=enable_ai_summary), enable_ai_summary
        return None, False

    @staticmethod
    def _prompt_enable_ai_summary_templates(databases: list[AnyDatabaseConfig]) -> bool:
        """Prompt whether ai_summary should be enabled for configured databases."""
        if not databases:
            return False

        return ask_confirm("Enable `ai_summary` template for all configured databases?", default=True)

    @staticmethod
    def _configure_ai_summary_templates(
        databases: list[AnyDatabaseConfig],
        llm: LLMConfig | None,
        enable_ai_summary: bool,
    ) -> list[AnyDatabaseConfig]:
        """Enable ai_summary template for configured databases when requested."""
        if not databases or llm is None or not enable_ai_summary:
            return databases

        for db in databases:
            if DatabaseTemplate.AI_SUMMARY not in db.templates:
                db.templates.append(DatabaseTemplate.AI_SUMMARY)

        return databases

    @staticmethod
    def _prompt_enable_profiling(databases: list[AnyDatabaseConfig]) -> bool:
        """Prompt whether column profiling should be enabled for configured databases."""
        if not databases:
            return False

        return ask_confirm(
            "Enable `profiling` template for all configured databases? (can be costly on large datasets)",
            default=False,
        )

    @staticmethod
    def _configure_profiling_templates(
        databases: list[AnyDatabaseConfig],
        enable_profiling: bool,
    ) -> list[AnyDatabaseConfig]:
        """Enable profiling template for configured databases when requested."""
        if not databases or not enable_profiling:
            return databases

        for db in databases:
            if DatabaseTemplate.PROFILING not in db.templates:
                db.templates.append(DatabaseTemplate.PROFILING)

        return databases

    @staticmethod
    def _prompt_slack() -> SlackConfig | None:
        """Prompt for Slack configuration using questionary."""
        if ask_confirm("Set up Slack integration?", default=False):
            return SlackConfig.promptConfig()
        return None

    @staticmethod
    def _prompt_notion() -> NotionConfig | None:
        """Prompt for Notion configuration using questionary."""
        if ask_confirm("Set up Notion integration?", default=False):
            return NotionConfig.promptConfig()
        return None

    @staticmethod
    def _prompt_mcp(project_name: str) -> McpConfig | None:
        """Prompt for MCP configuration using questionary."""
        if ask_confirm("Set up MCP servers?", default=False):
            McpConfig.promptConfig(project_name)
        return None

    @staticmethod
    def _prompt_skills(project_name: str) -> SkillsConfig | None:
        """Prompt for Skills configuration using questionary."""
        if ask_confirm("Set up Skills folder?", default=False):
            SkillsConfig.promptConfig(project_name)
        return None

    def save(self, path: Path) -> None:
        """Save the configuration to a YAML file."""
        config_file = path / "ca3_config.yaml"
        with config_file.open("w") as f:
            # Documentation Link
            f.write("# Configuration documentation:\n")
            f.write("# https://docs.getca3.io/ca3-agent/context-builder/configuration#nao_config-yaml\n\n")

            yaml.dump(
                self.model_dump(mode="json", by_alias=True, exclude_none=True),
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

    @classmethod
    def load(
        cls,
        path: Path,
        extra_env: dict[str, str] | None = None,
    ) -> "Ca3Config":
        """Load the configuration from a YAML file."""
        config_file = path / "ca3_config.yaml"
        content = config_file.read_text()
        processed_content, env_vars = cls._process_env_vars(content, extra_env=extra_env)
        cls._missing_env_vars = {k: None for k, v in env_vars.items() if v is None}
        data = yaml.safe_load(processed_content)
        return cls.model_validate(data)

    def get_connection(self, name: str) -> BaseBackend:
        """Get an Ibis connection by database name."""
        for db in self.databases:
            if db.name == name:
                return db.connect()
        raise ValueError(f"Database '{name}' not found in configuration")

    def get_all_connections(self) -> dict[str, BaseBackend]:
        """Get all Ibis connections as a dict keyed by name."""
        return {db.name: db.connect() for db in self.databases}

    @classmethod
    def try_load(
        cls,
        path: Path,
        *,
        exit_on_error: bool = False,
        raise_on_error: bool = False,
        extra_env: dict[str, str] | None = None,
    ) -> "Ca3Config | None":
        """Try to load config from path.

        Args:
            path: Directory containing ca3_config.yaml.
            exit_on_error: If True, prints error message and calls sys.exit(1) on failure.
            raise_on_error: If True, raises Ca3ConfigError on failure.
            extra_env: Optional env vars that take precedence over os.environ during template resolution.
        Returns:
            Ca3Config if loaded successfully, None if failed and both flags are False.
        """

        config_file = path / "ca3_config.yaml"

        def handle_error(message: str) -> None:
            if raise_on_error:
                raise Ca3ConfigError(message)
            if exit_on_error:
                console = Console()
                console.print(f"[bold red]✗[/bold red] {message}")
                sys.exit(1)

        if not config_file.exists():
            handle_error("No ca3_config.yaml found in current directory")
            return None

        try:
            os.chdir(path)
            return cls.load(path, extra_env=extra_env)
        except yaml.YAMLError as e:
            handle_error(f"Failed to load ca3_config.yaml: Invalid YAML syntax: {e}")
            return None
        except ValidationError as e:
            # Build detailed error message with suggestions
            main_errors = format_all_validation_errors(e, cls)
            msg = f"Failed to load ca3_config.yaml:\n  • {main_errors}"

            # Add warning about missing env vars if any
            if cls._missing_env_vars:
                env_var_warnings = "\n  • ".join(
                    f"{k} (environment variable not set or empty)" for k in cls._missing_env_vars.keys()
                )
                msg += f"\n\nWarning: Missing or empty environment variables:\n  • {env_var_warnings}"

            handle_error(msg)
            return None
        except ValueError as e:
            handle_error(f"Failed to load ca3_config.yaml: {e}")
            return None

    @classmethod
    def json_schema(cls) -> dict:
        """Generate JSON schema for the configuration."""
        return cls.model_json_schema()

    @staticmethod
    def _process_env_vars(
        content: str,
        extra_env: dict[str, str] | None = None,
    ) -> tuple[str, dict[str, str | None]]:
        """Support both ${{ env('VAR') }} and {{ env('VAR') }} formats.
        Returns:
            Tuple of (processed_content, env_var_status) where env_var_status maps
            env var names to their values (None if not set or empty)
        """
        regex = re.compile(r"\$?\{\{\s*env\(['\"]([^'\"]+)['\"]\)\s*\}\}")
        env_vars: dict[str, str | None] = {}

        def replacer(match: re.Match[str]) -> str:
            env_var = match.group(1)
            if extra_env is not None and env_var in extra_env:
                value = extra_env[env_var]
            else:
                value = os.environ.get(env_var)
            env_vars[env_var] = value if value else None
            return value or ""

        processed = regex.sub(replacer, content)
        return processed, env_vars


def resolve_project_path() -> Path:
    """Resolve the ca3 project directory from the current working directory."""
    return Path.cwd()
