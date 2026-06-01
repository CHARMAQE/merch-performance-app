import os

from config.env_loader import load_project_env


load_project_env()


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()

    return default


DB_CONFIG = {
    "host": _env("MYSQL_HOST", "DB_HOST", default="127.0.0.1"),
    "port": int(_env("MYSQL_PORT", "DB_PORT", default="3306")),
    "user": _env("MYSQL_USER", "DB_USER", default="root"),
    "password": _env("MYSQL_PASSWORD", "DB_PASSWORD", default="Root@123"),
    "database": _env("MYSQL_DATABASE", "DB_NAME", default="unilever_db"),
}
