import os
from typing import Any,Dict

import yaml

def load_config() -> Dict[str,Any]:
    config_path = os.environ.get("FDP_CONFIG", "/opt/fdp/config/dev.yml")

    with open(config_path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)