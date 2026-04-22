import os

from config.env_loader import load_project_env


load_project_env()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value.strip()


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost").strip(),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root").strip(),
    "password": _required_env("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "unilever_db").strip(),
}
