import os

__all__ = ["app"]

_MULTI_TENANT_SPACE_IDS = [
    "TangleML/tangle",
    "TangleML/tangle_multi_tenant",
    "Ark-kun/tangle_multi_tenant",
]

_is_multi_tenant = (
    os.environ.get("MULTI_TENANT", "false").lower() == "true"
    or os.environ.get("SPACE_ID") in _MULTI_TENANT_SPACE_IDS
)

if _is_multi_tenant:
    print("Starting multi-tenant mode")
    from start_HuggingFace_multi_tenant import app
else:
    print("Starting single-tenant mode")
    from start_HuggingFace_single_tenant import app

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
