"""

"""
# fmt:off
MSC = [
    "LRB", "LRC", "LRD", "LRE", "LRH", "LRL", "LRN", "LRP", 
    "MVD", "MVK", "MVM", "MVN", "MVP", "MVR", "MVS", 
    "NAB", "NAD", "NAE", "NAN", "NAO", "NAP",
    "NWD", "NWK", "NWO", "NWP", "NWS", "NWW", 
    "POA", "POD", "POH",
    "SAC", "SAD", "SAJ", "SAM", "SAS", "SAW",
    "SPA", "SPD", "SPK", "SPL", "SPN", 
    "SWD", "SWF", "SWG", "SWL", "SWT"
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
