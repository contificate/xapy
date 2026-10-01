"""Build both bindings; optionally publish unpublished schemas with GitHub CLI."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import venv

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def run(*args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def build(schema, output):
    version = schema.stem
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        work = Path(temporary)
        for module, distribution, asynchronous in (
            ("xapy", "xapy", False),
            ("xapy_async", "xapy-async", True),
        ):
            project = work / module
            package = project / module
            package.mkdir(parents=True)
            command = [sys.executable, str(ROOT / "main.py")]
            if asynchronous:
                command.append("--is-async")
            with (package / "__init__.py").open("w") as generated:
                run(*command, str(schema), stdout=generated)
            (package / "py.typed").touch()
            shutil.copyfile(package / "__init__.py", output / f"{module}.py")
            # The async preamble currently also imports requests.
            dependencies = ["requests", "aiohttp"] if asynchronous else ["requests"]
            (project / "pyproject.toml").write_text(f'''
[build-system]
requires = ["setuptools>=77"]
build-backend = "setuptools.build_meta"

[project]
name = "{distribution}"
version = "{version}"
description = "Generated {'asynchronous' if asynchronous else 'synchronous'} XenAPI bindings"
requires-python = ">=3.12"
dependencies = {json.dumps(dependencies)}

[tool.setuptools]
packages = ["{module}"]

[tool.setuptools.package-data]
{module} = ["py.typed"]
''')
            run(sys.executable, "-m", "build", "--outdir", str(output), str(project))

        # Check the installed wheels together, outside the repository, including
        # that each exposes the expected sync/async API and typing marker.
        environment = work / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = str(environment / "bin" / "python")
        run(python, "-m", "pip", "install", *map(str, output.glob("*.whl")))
        run(python, "-c", '''
import asyncio
import importlib.resources
import inspect
import xapy
import xapy_async

assert not inspect.iscoroutinefunction(xapy.Session.login_with_password)
assert inspect.iscoroutinefunction(xapy_async.Session.login_with_password)
for module in (xapy, xapy_async):
    assert importlib.resources.files(module).joinpath("py.typed").is_file()
    assert not module.Ref.NULL

class Connection:
    def call(self, method, params):
        assert method == "session.login_with_password"
        assert params == ["user", "password"]
        return "OpaqueRef:test"

class AsyncConnection(Connection):
    async def call(self, method, params, **kwargs):
        return super().call(method, params)

connection = Connection()
assert xapy.Session.login_with_password(connection, "user", "password").ref == "OpaqueRef:test"
connection = AsyncConnection()
assert asyncio.run(xapy_async.Session.login_with_password(connection, "user", "password")).ref == "OpaqueRef:test"
''', cwd=work)
    shutil.copyfile(schema, output / schema.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--schema", type=Path, help="Build only this versioned JSON")
    args = parser.parse_args()
    schemas = [args.schema.resolve()] if args.schema else list((ROOT / "schemas").glob("*.json"))
    for schema in schemas:
        if not VERSION.fullmatch(schema.stem):
            parser.error(f"Expected a major.minor.patch.json filename: {schema.name}")
    schemas.sort(key=lambda path: tuple(map(int, path.stem.split("."))))
    releases = {}
    if args.publish:
        repository = os.environ["GITHUB_REPOSITORY"]
        commit = os.environ["GITHUB_SHA"]
        pages = json.loads(run(
            "gh", "api", "--paginate", "--slurp",
            f"repos/{repository}/releases?per_page=100", capture_output=True,
        ).stdout)
        releases = {release["tag_name"]: release for page in pages for release in page}

    for schema in schemas:
        tag = f"v{schema.stem}"
        release = releases.get(tag)
        if release and not release["draft"]:
            print(f"Skipping published release {tag}", flush=True)
            continue
        output = ROOT / "dist" / schema.stem
        # A fresh staging directory prevents stale files from entering a release.
        with tempfile.TemporaryDirectory() as temporary:
            staged = Path(temporary) / "dist"
            build(schema, staged)
            if args.publish:
                notes = (
                    f"Bindings generated from `{schema.name}`. Requires Python 3.12+.\n\n"
                    f"Generator commit: {commit}\n\n"
                    f"Schema SHA-256: {hashlib.sha256(schema.read_bytes()).hexdigest()}\n\n"
                    "Install the xapy wheel for `import xapy`, or the xapy_async wheel "
                    "for `import xapy_async`. Both can be installed together."
                )
                if release:
                    # Retry only drafts from this same commit, so the tag and
                    # generated contents cannot silently diverge.
                    if release["target_commitish"] != commit:
                        raise RuntimeError(f"Draft {tag} belongs to another commit; resolve it before retrying")
                else:
                    run("gh", "release", "create", tag, "--repo", repository,
                        "--target", commit, "--title", f"XenAPI bindings {schema.stem}",
                        "--notes", notes, "--draft")
                run("gh", "release", "upload", tag, "--repo", repository,
                    "--clobber", *map(str, sorted(staged.iterdir())))
                run("gh", "release", "edit", tag, "--repo", repository, "--draft=false")
            output.mkdir(parents=True, exist_ok=True)
            for artifact in staged.iterdir():
                shutil.copyfile(artifact, output / artifact.name)
            print(f"Built {schema.name}: {output}", flush=True)
    if not schemas:
        print("No versioned schemas yet; add schemas/26.1.0.json to create a release.")


if __name__ == "__main__":
    main()
