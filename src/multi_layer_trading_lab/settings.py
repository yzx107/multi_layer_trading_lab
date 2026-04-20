from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    environment: str = "dev"
    data_root: Path = Field(default=Path("data"))
    lake_root: Path = Field(default=Path("data/lake"))
    market: str = "HK"
    tushare_token: str | None = None
    futu_host: str = "127.0.0.1"
    futu_port: int = 11111
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    execution_mode: str = "dry_run"


settings = Settings()
