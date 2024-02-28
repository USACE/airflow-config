# from requests_toolbelt import SSLAdapter
import requests

# import ssl

# s = requests.Session()
# s.mount("https://", SSLAdapter(ssl.PROTOCOL_SSLv23))

# print(s.get("https://cwms-data.usace.army.mil/cwms-data/states"))

import ssl

ssl.PROTOCOL_SSLv23 = ssl.PROTOCOL_TLSv1

x = requests.get("https://cwms-data-test.cwbi.us/cwms-data/states")
print(x.status_code)

# import requests
# from requests.adapters import HTTPAdapter
# from urllib3 import PoolManager

# # from requests.packages.urllib3.util.ssl_ import create_urllib3_context
# from urllib3.util import create_urllib3_context

# ssl_context = create_urllib3_context()


# # Print SSLContext attributes
# print("SSLContext attributes:")
# print(f"Protocol: {ssl_context.protocol}")
# # print(f"Ciphers: {ssl_context.ciphers}")
# print(f"Options: {ssl_context.options}")
# print(f"Verify Mode: {ssl_context.verify_mode}")
# print(f"Load Default CAs: {ssl_context.check_hostname}")
# # Add more attributes as needed

# # If you want to see all attributes and methods of SSLContext, you can use dir()
# print("\nAll attributes and methods:")
# print(dir(ssl_context))

"""
ssl_context.options |= getattr(ssl_context, "_options_map")["OP_NO_SSLv3"]
ssl_context.set_ciphers("DEFAULT@SECLEVEL=1")

adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=3)
session = requests.Session()
session.mount("https://", adapter)

response = session.get(
    "https://cwms-data.usace.army.mil/cwms-data/states", verify=False
)
"""

# ctx = create_urllib3_context()
# ctx.load_default_certs()
# ctx.options |= ssl.OP_ENABLE_MIDDLEBOX_COMPAT

# with PoolManager(ssl_context=ctx) as pool:
#     pool.request("GET", "https://cwms-data.usace.army.mil/cwms-data/states")
