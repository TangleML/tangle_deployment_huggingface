# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# HuggingFace dev mode requires wget, git
RUN apt-get update && \
    apt-get install -y bash curl wget procps git git-lfs && \
    rm -rf /var/lib/apt/lists/*

# Creating the /data directory and giving full access to users to avoid the errors:
# --> RUN mkdir -p /data
# mkdir: cannot create directory ‘/data’: Permission denied
RUN mkdir -p /data
RUN chmod 777 /data

# # Setup a non-root user
# RUN groupadd --system --gid 999 nonroot \
#  && useradd --system --gid 999 --uid 999 --create-home nonroot

# The two following lines are requirements for the Dev Mode to be functional
# Learn more about the Dev Mode at https://huggingface.co/dev-mode-explorers
RUN useradd -m -u 1000 user
USER user

# Install the project into `/app`
WORKDIR /app/backend

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=backend/uv.lock,target=uv.lock \
    --mount=type=bind,source=backend/pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
# COPY backend /app/backend
COPY --chown=user . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


# Installing HuggingFace. Needs to be done after uv sync
RUN uv pip install huggingface_hub[oauth]

# Place executables in the environment at the front of the path
ENV PATH="/app/backend/.venv/bin:$PATH"

# Adding HF-only experimental files
COPY huggingface_overlay /app/backend


# Copy frontend build
# COPY frontend_build /app/frontend_build

# Put Tangle data into persistent storage
RUN mkdir -p /data
RUN ln -s /data/tangle/data /app/backend/data


# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# # Use the non-root user to run our application
# USER nonroot

# Run the FastAPI application by default
# Uses `fastapi dev` to enable hot-reloading when the `watch` sync occurs
# Uses `--host 0.0.0.0` to allow access from outside the container
# Note in production, you should use `fastapi run` instead

# WORKDIR /app
# CMD ["fastapi", "dev", "--host", "0.0.0.0", "/app/backend/start_local.py"]

# WORKDIR /app/backend
# CMD ["fastapi", "dev", "--host", "0.0.0.0", "/app/start_HuggingFace.py"]

WORKDIR /app/backend
CMD ["fastapi", "dev", "--host", "0.0.0.0", "--port", "7860", "/app/backend/start_HuggingFace.py"]
