import os
from pathlib import Path

import psycopg2
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine


def _load_env() -> None:
    load_dotenv()


def _resolve_env(value: str) -> str:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        key = value[2:-1]
        return os.getenv(key, "")
    return value


def get_db_config() -> dict:
    _load_env()
    config_path = Path("config/config.yaml")
    config = {}
    if config_path.exists():
        with config_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
            config = raw.get("database", {})
    return {
        "host": os.getenv("DB_HOST", _resolve_env(config.get("host", "localhost"))),
        "port": int(os.getenv("DB_PORT", _resolve_env(config.get("port", "5432")) or 5432)),
        "name": os.getenv("DB_NAME", _resolve_env(config.get("name", "ecommerce_db"))),
        "user": os.getenv("DB_USER", _resolve_env(config.get("user", "postgres"))),
        "password": os.getenv("DB_PASSWORD", _resolve_env(config.get("password", "postgres"))),
    }


def get_connection():
    cfg = get_db_config()
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["name"],
        user=cfg["user"],
        password=cfg["password"],
    )


def get_engine():
    cfg = get_db_config()
    url = (
        f"postgresql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['name']}"
    )
    return create_engine(url)


def get_connection_string():
    cfg = get_db_config()
    return (
        f"postgresql://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg['port']}/{cfg['name']}"
    )
