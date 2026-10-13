"""The alert rules file must be well formed, use only metrics QueueForge really exposes, and
every alert must point at a runbook section that exists."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
RULES = yaml.safe_load((ROOT / "deploy/monitoring/alerts.yml").read_text(encoding="utf-8"))
METRICS_SOURCE = (ROOT / "src/queueforge/api/metrics.py").read_text(encoding="utf-8")
RUNBOOK = (ROOT / "docs/runbooks.md").read_text(encoding="utf-8")


def all_rules() -> list[dict[str, object]]:
    return [rule for group in RULES["groups"] for rule in group["rules"]]


def test_every_alert_is_complete() -> None:
    rules = all_rules()
    assert len(rules) >= 6
    names = [str(r["alert"]) for r in rules]
    assert len(names) == len(set(names)), "alert names must be unique"
    for rule in rules:
        assert rule["expr"], rule["alert"]
        assert rule["for"], f"{rule['alert']} needs a 'for' so a blip does not page anyone"
        labels = rule["labels"]
        annotations = rule["annotations"]
        assert isinstance(labels, dict) and labels["severity"] in {"warning", "critical"}
        assert isinstance(annotations, dict)
        assert annotations["summary"] and annotations["description"]


def test_alerts_only_use_metrics_that_exist() -> None:
    exposed = set(re.findall(r'"(queueforge_[a-z_]+)"', METRICS_SOURCE))
    assert exposed, "could not read the metric names from metrics.py"
    for rule in all_rules():
        used = set(re.findall(r"queueforge_[a-z_]+", str(rule["expr"])))
        for name in used:
            assert name in exposed, f"{rule['alert']} uses {name}, which /metrics does not expose"


def test_every_alert_has_a_runbook_section() -> None:
    for rule in all_rules():
        annotations = rule["annotations"]
        assert isinstance(annotations, dict)
        assert annotations["runbook"] == f"docs/runbooks.md#{str(rule['alert']).lower()}"
        heading = rf"^## {rule['alert']}\s*$"
        assert re.search(heading, RUNBOOK, re.MULTILINE), f"no runbook for {rule['alert']}"
