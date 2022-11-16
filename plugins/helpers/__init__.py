"""

"""
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook


# fmt:off
# NWx at end of list to force lower priority
MSC = [
    "LRB", "LRC", "LRD", "LRE", "LRH", "LRL", "LRN", "LRP", 
    "MVD", "MVK", "MVM", "MVN", "MVP", "MVR", "MVS", 
    "NAB", "NAD", "NAE", "NAN", "NAO", "NAP",
    "POA", "POD", "POH",
    "SAC", "SAD", "SAJ", "SAM", "SAS", "SAW",
    "SPA", "SPD", "SPK", "SPL", "SPN", 
    "SWD", "SWF", "SWG", "SWL", "SWT"
    "NWD", "NWK", "NWO", "NWP", "NWS", "NWW",
]

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]
# fmt: on


def usace_office_group(office):
    try:
        office = {
            "NWK": "NWDM",
            "NWO": "NWDM",
            "NWP": "NWDP",
            "NWS": "NWDP",
            "NWW": "NWDP",
            "NWD": "NWD",
        }[office.upper()]
    except KeyError as err:
        print(f"KeyError: key not found - {err}; continue with '{office}'")
    finally:
        return office


class HelperHook(HttpHook):
    """
    method: str = 'POST'
    http_conn_id: str = default_conn_name
    auth_type: Any = HTTPBasicAuth
    tcp_keep_alive: bool = True
    tcp_keep_alive_idle: int = 120
    tcp_keep_alive_count: int = 20
    tcp_keep_alive_interval: int = 30
    """

    def __init__(self, *args, **kw):

        self.args = args
        self.kw = kw

        self.response_type = "json"

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        super().__init__(*args, **kw)

    def request(self, *args, **kw):
        """
        endpoint: str | None = None
        data: Dict[str, Any] | str | None = None
        headers: Dict[str, Any] | None = None
        extra_options: Dict[str, Any] | None = None
        **request_kwargs: Any
        """

        if self.kw["method"] in ["POST", "PUT"]:
            if len(args) >= 1:
                args = list(args)
                args[0] += f"?key={self.conn.password}"
            if "endpoint" in kw:
                kw["endpoint"] += f"?key={self.conn.password}"

        resp = self.run(*args, **kw)
        if resp.status_code in [200, 201, 202]:
            if self.response_type == "json":
                return resp.json()
            elif self.response_type == "text":
                return resp.text