from typing import Any, Dict, List
from airflow.models import Variable
import json


def get_office_groups(offices: List[str]) -> Dict[str, List[Dict[str, Any]]]:

    OFFICE_GROUPS = json.loads(Variable.get("OFFICE_GROUPS"))
    job_configs = [
        entry for entry in OFFICE_GROUPS if entry.get("office") in offices]

    # Organize configs by office_group
    groups = {}
    for config in job_configs:
        groups.setdefault(config["office_group"], []).append(config)

    return groups
