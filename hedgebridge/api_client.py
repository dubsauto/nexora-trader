# hedgebridge/api_client.py

import os
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi

load_dotenv()

API_TOKEN = os.getenv("ACCESS_TOKEN")

_metaapi_client: MetaApi | None = None


def get_metaapi_client() -> MetaApi:
    global _metaapi_client

    if _metaapi_client is None:
        if not API_TOKEN:
            raise ValueError("❌ ACCESS_TOKEN is not set in environment")

        print("🚀 Initializing MetaApi client...")
        _metaapi_client = MetaApi(API_TOKEN)

    return _metaapi_client


def reset_metaapi_client() -> MetaApi:
    """
    Replace the global MetaApi singleton with a fresh instance.

    Callers are responsible for closing the *old* instance before or after
    calling this function — the pool's _reset_sdk_safely() does this
    properly so that WebSocket threads and connections are freed rather
    than orphaned.
    """
    global _metaapi_client

    if not API_TOKEN:
        raise ValueError("❌ ACCESS_TOKEN is not set in environment")

    print("🔄 Creating fresh MetaApi client...")
    _metaapi_client = MetaApi(API_TOKEN)
    return _metaapi_client
