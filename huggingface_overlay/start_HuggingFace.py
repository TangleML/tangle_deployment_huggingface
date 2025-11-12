import os

__all__ = ["app"]

print("Starting single-tenant mode")
from start_HuggingFace_single_tenant import app

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
