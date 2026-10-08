"""Real Git history receipts; these fixtures are not performance results."""

from copy import deepcopy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

import workspace_update_campaign as campaign
import workspace_update_campaign_export as export
import workspace_update_git_performance as git_performance


class HistoryReplayTests(unittest.TestCase):
    def test_ten_real_commits_are_prepared_before_measurement_and_replayed(self):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            performance = root / "git-performance.jsonl"
            stack.enter_context(patch.dict(os.environ, {git_performance.PATH_ENV: str(performance)}))
            stack.enter_context(git_performance.context(stage="setup", phase="fair", round=1,
                                                      client=None, version=None))
            repo = root / "fixture"
            subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
            deadline = time.monotonic() + 120
            env = campaign.common.clean_env({"GIT_AUTHOR_NAME": "test", "GIT_COMMITTER_NAME": "test",
                "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_EMAIL": "test@example.invalid"})
            campaign.common.git(repo, deadline, "commit", "--allow-empty", "-qm", "seed", env=env)
            seed = campaign.common.git(repo, deadline, "rev-parse", "HEAD").decode().strip()
            folder = root / "measurements/fair/round-01"
            folder.mkdir(parents=True)
            with patch.dict(campaign.common.fixture_size.PROFILES, {"history-large": (40, 1, 2, 1024)}):
                commits = campaign.prepare_history(repo, folder, "history-large", 1, seed, deadline, fixture_round=1)
                self.assertEqual(list(commits), [f"v{number}" for number in range(1, 11)])
                self.assertEqual(len({commit for commit, _ in commits.values()}), 10)
                receipt_path = folder / "git-history.json"
                original = receipt_path.read_bytes()
                history = json.loads(original)
                records = [{"round": 1, "version": version, "fixed_commit": commit, "path_tree": tree,
                    "oracle_manifest_sha256": export.hashlib.sha256((folder / f"{version}-expected.json").read_bytes()).hexdigest()}
                    for version, (commit, tree) in commits.items() for _ in ("a", "b")]
                self.assertEqual(export.validate_history(root, "fair", 1, seed, records, "history-large"), history)
                self.assertTrue(all(export.allowed(str(path.relative_to(root))) for path in folder.iterdir()))
                for mutation in ("missing_tenth", "duplicate_ninth", "lexical_order", "parent", "body", "counts", "source_counts", "native_commit"):
                    altered = deepcopy(history)
                    altered_records = deepcopy(records)
                    if mutation == "missing_tenth":
                        altered["commits"].pop()
                    elif mutation == "duplicate_ninth":
                        altered["commits"][-1] = deepcopy(altered["commits"][-2])
                    elif mutation == "lexical_order":
                        altered["commits"].sort(key=lambda entry: entry["version"])
                    elif mutation == "parent":
                        altered["commits"][-1]["parent"] = seed
                    elif mutation == "body":
                        altered["commits"][-1]["commit_body_hex"] += "00"
                    elif mutation == "counts":
                        altered["commits"][-1]["change_counts"]["files_modified"] = 0
                    elif mutation == "source_counts":
                        altered["commits"][-1]["source_changes"]["changed_source_bytes"] = 0
                    else:
                        altered_records[-1]["fixed_commit"] = seed
                    receipt_path.write_text(json.dumps(altered), encoding="utf-8")
                    with self.subTest(mutation=mutation), self.assertRaises(AssertionError):
                        export.validate_history(root, "fair", 1, seed, altered_records, "history-large")
                receipt_path.write_bytes(original)
                last_manifest = folder / "v10-expected.json"
                last_manifest.write_bytes((folder / "v9-expected.json").read_bytes())
                with self.assertRaises((AssertionError, ValueError)):
                    export.validate_history(root, "fair", 1, seed, records, "history-large")
                git_performance.finalize(performance)
                events = git_performance.read_records(performance)
                commands = [row for row in events if row["event"] == "end"]
                self.assertTrue(commands)
                self.assertTrue(all(row["status"] == "completed" for row in commands))
                for version in campaign.common.scenarios("history-large"):
                    per_version = [row for row in commands if row["context"]["version"] == version]
                    self.assertTrue(per_version, version)
                    self.assertTrue(all(row["context"]["stage"] == "fixture" for row in per_version))
                    self.assertTrue({"commit-tree", "update-ref", "ls-tree", "cat-file"}
                                    <= {row["operation"] for row in per_version})


if __name__ == "__main__":
    unittest.main()
