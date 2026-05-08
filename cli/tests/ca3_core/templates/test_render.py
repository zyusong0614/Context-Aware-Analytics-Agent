"""Unit tests for the user template renderer (render.py)."""

import json
from pathlib import Path
from unittest.mock import patch

from ca3_core.templates.render import render_template


class TestRenderTemplateToJsonFilter:
    """Tests for the to_json filter inside render_template."""

    def test_to_json_preserves_non_ascii(self, tmp_path: Path):
        """to_json filter in render_template preserves non-ASCII characters."""
        template = tmp_path / "test.md.j2"
        template.write_text("{{ ca3.data | to_json }}")

        mock_context = {"data": {"name": "テスト", "emoji": "🎉"}}

        with patch("ca3_core.templates.render.create_ca3_context") as mock_create:
            mock_ca3 = type("CA3", (), mock_context)()
            mock_create.return_value = mock_ca3

            output_path = render_template(
                template_path=Path("test.md.j2"),
                project_path=tmp_path,
                config=None,  # type: ignore[arg-type]
            )

        rendered = output_path.read_text()
        assert "テスト" in rendered
        assert "🎉" in rendered
        assert "\\u" not in rendered
        parsed = json.loads(rendered)
        assert parsed == {"name": "テスト", "emoji": "🎉"}

    def test_to_json_non_ascii_roundtrips(self, tmp_path: Path):
        """Non-ASCII data round-trips through to_json correctly."""
        template = tmp_path / "test.md.j2"
        template.write_text("{{ ca3.rows | to_json(indent=2) }}")

        rows = [
            {"id": 1, "city": "東京"},
            {"id": 2, "city": "서울"},
            {"id": 3, "city": "القاهرة"},
        ]
        mock_context = {"rows": rows}

        with patch("ca3_core.templates.render.create_ca3_context") as mock_create:
            mock_ca3 = type("CA3", (), mock_context)()
            mock_create.return_value = mock_ca3

            output_path = render_template(
                template_path=Path("test.md.j2"),
                project_path=tmp_path,
                config=None,  # type: ignore[arg-type]
            )

        rendered = output_path.read_text()
        assert "東京" in rendered
        assert "서울" in rendered
        assert "القاهرة" in rendered
        assert "\\u" not in rendered
        parsed = json.loads(rendered)
        assert parsed == rows
