import os
from pathlib import Path


ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def _parse_env_line(line: str):
    line = line.strip()

    if not line or line.startswith("#"):
        return None, None

    if "=" not in line:
        return None, None

    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()

    if not key:
        return None, None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]

    return key, value


def load_project_env(env_path=None, override=False):
    path = Path(env_path).resolve() if env_path else ENV_FILE

    if not path.exists():
        return path

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        key, value = _parse_env_line(raw_line)

        if not key:
            continue

        if override or key not in os.environ:
            os.environ[key] = value

    return path
