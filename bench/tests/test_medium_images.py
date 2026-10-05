import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class MediumImageTests(unittest.TestCase):
    def test_render_rewrites_every_backend_service_image(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "stack.json"
            subprocess.run([sys.executable, str(ROOT / "bench/infra/render-medium-stack.py"),
                            "--runner", "runner:test", "--backend", "backend:frozen",
                            "--fixture", "fixture:test", "--out", str(output)], check=True,
                           stdout=subprocess.PIPE)
            docs = json.loads(output.read_text())["items"]
            services = {d["metadata"]["name"]: d for d in docs
                        if d["kind"] in ("Deployment", "Job")}
            for name in ("postgres", "redis", "rustfs", "rustfs-init", "mega2"):
                spec = services[name]["spec"]["template"]["spec"]
                for c in spec.get("containers", []) + spec.get("initContainers", []):
                    self.assertEqual(c["image"], "backend:frozen", name)
            self.assertNotIn("mega2:local", json.dumps(docs))

    def test_build_context_supplies_runtime_assets_without_base_image_assumptions(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp)
            (run / "repo/.git").mkdir(parents=True)
            for name in ("manifest.jsonl", "manifest.refs.json", "manifest.summary.json", "repo/.git/HEAD"):
                (run / name).write_text("fixture")
            binaries = run / "bin"
            binaries.mkdir()
            for name in ("scorpio", "antares", "libra"):
                path = binaries / name
                path.write_text("#!/bin/sh\nexit 0\n")
                path.chmod(0o755)
            # Stop at the first Docker call, leaving the real generated build context for inspection.
            docker = binaries / "docker"
            docker.write_text("#!/bin/sh\nexit 42\n")
            docker.chmod(0o755)
            env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                       MEDIUM_WORKDIR=str(run), MEDIUM_RELEASE_DIR=str(binaries),
                       MEDIUM_LIBRA_BINARY=str(binaries / "libra"), MEDIUM_REGISTRY="example/test",
                       MEDIUM_TAG="test", MEDIUM_BASE_IMAGE="base:test", MEDIUM_BACKEND_IMAGE="backend:test")
            result = subprocess.run(["bash", str(ROOT / "bench/infra/build-medium-images.sh")], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.assertEqual(result.returncode, 42, result.stderr.decode())
            context = run / "runtime-context"
            for name in ("scorpio", "antares", "libra", "docker-entrypoint.sh", "scorpio.toml"):
                self.assertTrue((context / name).is_file(), name)
            self.assertEqual((context / "docker-entrypoint.sh").read_bytes(),
                             (ROOT / "deploy/docker-entrypoint.sh").read_bytes().replace(b"\r\n", b"\n"))
            self.assertEqual((context / "scorpio.toml").read_bytes(),
                             (ROOT / "scorpio.toml.example").read_bytes())
            dockerfile = (context / "Dockerfile").read_text()
            self.assertIn("/usr/local/bin/docker-entrypoint.sh --help", dockerfile)
            self.assertIn("libra --help", dockerfile)
            self.assertIn("SCORPIO_CONFIG_FILE=", dockerfile)


if __name__ == "__main__":
    unittest.main()
