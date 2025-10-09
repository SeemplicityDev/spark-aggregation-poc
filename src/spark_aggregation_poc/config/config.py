import os
from dataclasses import dataclass
from os import path

import yaml


@dataclass
class Config:
    postgres_url: str
    postgres_properties: dict
    catalog_table_prefix: str = None
    customer: str = None

class ConfigLoader:
    @classmethod
    def load_config(cls) -> Config:
        # Get the directory where this config.py file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "config.yaml")

        with open(config_path) as f:
            raw = yaml.safe_load(f)
            return Config(postgres_url=raw["postgres"]["postgres_url"], postgres_properties=raw["postgres"]["postgres_properties"])
            # return AppConfig(postgres=PostgresConfig(**raw["postgres"]))
