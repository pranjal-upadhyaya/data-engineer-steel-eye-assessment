import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseSettings):
    """Application configuration loaded from environment variables or a .env file."""

    base_folder: str = "temp"
    csv_storage_path: str = ""  # fsspec URL for cloud upload, e.g. s3://bucket/csv or abfs://container@account.dfs.core.windows.net/csv

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )

    @property
    def xml_folder(self) -> str:
        """Return the path to the folder where downloaded XML files are stored."""
        return os.path.join(self.base_folder, "xml")

    @property
    def csv_folder(self) -> str:
        """Return the local path where CSV files are written during processing."""
        return os.path.join(self.base_folder, "csv")


app_config = AppConfig()
