from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).resolve().parent.parent / "app" / "static"
HTML = (STATIC / "index.html").read_text(encoding="utf-8")
JS = (STATIC / "app.js").read_text(encoding="utf-8")


def test_every_referenced_element_id_exists():
    referenced = set(re.findall(r'\$\("([\w-]+)"\)', JS))
    referenced |= set(re.findall(r'getElementById\("([\w-]+)"\)', JS))
    declared = set(re.findall(r'id="([\w-]+)"', HTML))
    missing = referenced - declared
    assert not missing, f"JS references missing ids: {sorted(missing)}"


def test_ids_are_unique():
    ids = re.findall(r'id="([\w-]+)"', HTML)
    assert len(ids) == len(set(ids))


def test_api_endpoints_used_by_ui_exist():
    used = set(re.findall(r'fetch\("(/api/[\w/-]+)"', JS))
    assert used == {"/api/status", "/api/search", "/api/settings"}


def test_page_mentions_required_controls():
    for needle in ("searchForm", "readyTimeout", "startupDelay", "responseDelay", "stateBadge"):
        assert needle in HTML
    assert "就绪等待上限" in HTML
