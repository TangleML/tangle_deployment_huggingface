import logging
import os
import pathlib
import typing

import fastapi
import huggingface_hub
import huggingface_hub.errors

ENABLE_HUGGINGFACE_AUTH = True


# region Paths configuration

root_data_dir: str = "./data/"
root_data_dir_path = pathlib.Path(root_data_dir).resolve()
print(f"{root_data_dir_path=}")

# artifacts_dir_path = root_data_dir_path / "artifacts"
# logs_dir_path = root_data_dir_path / "logs"

root_data_dir_path.mkdir(parents=True, exist_ok=True)
# artifacts_dir_path.mkdir(parents=True, exist_ok=True)
# logs_dir_path.mkdir(parents=True, exist_ok=True)
# endregion

# region: DB Configuration
database_path = root_data_dir_path / "db.sqlite"
database_uri = f"sqlite:///{database_path}"
print(f"{database_uri=}")
# endregion

# region: Storage configuration
# from cloud_pipelines.orchestration.storage_providers import local_storage
# storage_provider = local_storage.LocalStorageProvider()
from cloud_pipelines_backend.storage_providers import huggingface_repo_storage

storage_provider = huggingface_repo_storage.HuggingFaceRepoStorageProvider()

# artifacts_root_uri = artifacts_dir_path.as_posix()
# logs_root_uri = logs_dir_path.as_posix()
artifacts_root_uri = os.environ.get("DATA_DIR_URI")
logs_root_uri = artifacts_root_uri
# endregion

# region: Launcher configuration
from cloud_pipelines_backend.launchers import huggingface_launchers

# Requires HF_TOKEN
launcher = huggingface_launchers.HuggingFaceJobsContainerLauncher()

# endregion

# region: Orchestrator configuration
default_task_annotations = {}
sleep_seconds_between_queue_sweeps: float = 5.0
# endregion

# region: Authentication configuration
import fastapi

print(f"{os.environ=}")

print(f'{os.environ["PERSISTENT_STORAGE_ENABLED"]=}')

hf_space_author_name = os.environ.get("SPACE_AUTHOR_NAME")
hf_space_creator_user_id = os.environ.get("SPACE_CREATOR_USER_ID")
print(f"{hf_space_author_name=}")
print(f"{hf_space_creator_user_id=}")

hf_token: str | None = None
try:
    hf_token = huggingface_hub.get_token()
except Exception as ex:
    logging.error("Error in `huggingface_hub.get_token()`")

print(f"{(hf_token is not None)=}")

hf_whoami: dict | None = None
hf_whoami_user_name: str | None = None
try:
    hf_whoami = huggingface_hub.whoami()
    hf_whoami_user_name = hf_whoami.get("name") if hf_whoami else None
except Exception as ex:
    logging.error("Error in `hugginface_hub.whoami()`")

print(f"{hf_whoami=}")
print(f"{hf_whoami_user_name=}")


# ! This function is just a placeholder for user authentication and authorization so that every request has a user name and permissions.
# ! This placeholder function authenticates the user as user with name "admin" and read/write/admin permissions.
# ! In a real multi-user deployment, the `get_user_details` function MUST be replaced with real authentication/authorization based on OAuth or another auth system.
# ADMIN_USER_NAME = "admin"

# FIX: Set to False by default
# any_user_can_read = os.environ.get("ANY_USER_CAN_READ", "false").lower() == "true"
any_user_can_read = os.environ.get("ANY_USER_CAN_READ", "true").lower() == "true"
print(f"{any_user_can_read=}")

IS_HUGGINGFACE_SPACE = hf_space_author_name is not None
print(f"{IS_HUGGINGFACE_SPACE=}")

if IS_HUGGINGFACE_SPACE:
    ADMIN_USER_NAME = hf_space_author_name
    print(f"{ADMIN_USER_NAME=}")

    default_component_library_owner_username = ADMIN_USER_NAME

    # Single-tenant
    # Selecting the tenant. It's the user or arg that host the space.
    tenant_name = hf_space_author_name

    # We need to be careful and prevent public spaces with HF_TOKEN set from letting anyone exploit the HF_TOKEN user.
    def get_user_details(request: fastapi.Request):
        user_can_read = False
        user_can_write = False
        user_can_admin = False
        user_can_read = user_can_read or any_user_can_read

        oauth_info = huggingface_hub.parse_huggingface_oauth(request)
        # if "USER_PERMISSIONS_MAP" in os.environ:
        #     ...

        if oauth_info:
            logger.info(f"{oauth_info=}")
            logger.info(f"{oauth_info.user_info=}")
            logger.info(f"{oauth_info.user_info.is_pro=}")
            logger.info(f"{oauth_info.user_info.can_pay=}")
            # TODO: Allow access for users belonging to an allowed org

            user_is_space_author = (
                oauth_info.user_info.preferred_username == hf_space_author_name
            )
            user_is_space_author_by_id = (
                oauth_info.user_info.sub == hf_space_creator_user_id
            )
            # oauth_info.user_info.orgs[0].role_in_org
            user_belongs_to_space_org = any(
                org.preferred_username == hf_space_author_name
                for org in oauth_info.user_info.orgs or []
            )
            logger.info(f"{user_belongs_to_space_org=}")
            logger.info(f"{user_is_space_author=}")
            logger.info(f"{user_is_space_author_by_id=}")

            user_can_write = user_can_write or user_is_space_author
            user_can_admin = user_can_admin or user_is_space_author

            try:
                # Checking user's role in the space org:
                # For some reason, in OAuth_info, orgs are always empty.
                # Getting the info using whoami
                # This leads to extra HF API requests. Find a better way to fix.
                logger.info(f"{huggingface_hub.whoami(token=oauth_info.access_token)=}")
                oauth_whoami_user_info = huggingface_hub.whoami(
                    token=oauth_info.access_token
                )
                user_orgs = oauth_whoami_user_info.get("orgs", [])
                space_org_candidates = [
                    user_org
                    for user_org in user_orgs
                    # Does not work: hf_space_creator_user_id is the creator user ID, not the space org ID
                    # if user_org.get("id") == hf_space_creator_user_id
                    if user_org.get("name") == hf_space_author_name
                ]
                if space_org_candidates:
                    space_org = space_org_candidates[0]
                    logger.info(f"{space_org=}")
                    user_role_in_org = space_org.get("roleInOrg")
                    logger.info(f"{user_role_in_org=}")

                    if user_role_in_org == "admin":
                        user_can_read = True
                        user_can_write = True
                        user_can_admin = True
                    elif user_role_in_org in ("write", "contribute"):
                        user_can_read = True
                        user_can_write = True
                    elif user_role_in_org == "read":
                        user_can_read = True
                    else:
                        pass

                user_details = api_router.UserDetails(
                    name=oauth_info.user_info.preferred_username,
                    permissions=api_router.Permissions(
                        read=user_can_read,
                        write=user_can_write,
                        admin=user_can_admin,
                    ),
                )
                logger.info(f"{user_details=}")
                return user_details
            except huggingface_hub.errors.HfHubHTTPError as ex:
                # Maybe redirect to logout or login API?
                # Does not work. The browser is not redirected
                # logger.error(
                #     f"Error getting authentication info from HuggingFace. Redirecting to login",
                #     exc_info=True,
                # )
                # raise fastapi.HTTPException(
                #     status_code=302,
                #     detail="Authorization error",
                #     # headers={"Location": "/api/oauth/huggingface/logout"},
                #     headers={"Location": "/api/oauth/huggingface/login"},
                # )
                if ex.response and ex.response.status_code == 401:
                    logger.error(
                        f"Error getting authentication info from HuggingFace. Deleting session OAuth info",
                        exc_info=True,
                    )
                    request.session.pop("oauth_info", None)
                else:
                    logger.error(
                        f"Error getting authentication info from HuggingFace.",
                        exc_info=True,
                    )

        return api_router.UserDetails(
            name="anonymous",
            permissions=api_router.Permissions(
                read=any_user_can_read,
                write=False,
                admin=False,
            ),
        )

else:
    # We're not in space.
    ADMIN_USER_NAME = hf_whoami_user_name or "admin"
    print(f"{ADMIN_USER_NAME=}")

    default_component_library_owner_username = ADMIN_USER_NAME

    # We need to be careful and prevent public spaces with HF_TOKEN set from letting anyone exploit the HF_TOKEN user.
    def get_user_details(request: fastapi.Request):
        return api_router.UserDetails(
            name=ADMIN_USER_NAME,
            permissions=api_router.Permissions(
                read=True,
                write=True,
                admin=True,
            ),
        )


# !!! TODO: Use authenticated user's token to run Jobs via launcher.

# endregion


# region: Logging configuration
import logging.config

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        # root logger
        "": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": False,
        },
        "uvicorn.error": {
            "level": "DEBUG",
            "handlers": ["default"],
            # Fix triplicated log messages
            "propagate": False,
        },
        "uvicorn.access": {
            "level": "DEBUG",
            "handlers": ["default"],
        },
        "watchfiles.main": {
            "level": "WARNING",
            "handlers": ["default"],
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger(__name__)
# endregion

# region: Database engine initialization
from cloud_pipelines_backend import database_ops

db_engine = database_ops.create_db_engine(
    database_uri=database_uri,
)
# endregion


# region: Orchestrator initialization

import logging
import pathlib

import sqlalchemy
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_interfaces,
)
from cloud_pipelines_backend import orchestrator_sql


def run_orchestrator(
    db_engine: sqlalchemy.Engine,
    storage_provider: storage_interfaces.StorageProvider,
    data_root_uri: str,
    logs_root_uri: str,
    sleep_seconds_between_queue_sweeps: float = 5.0,
):
    # logger = logging.getLogger(__name__)
    # orchestrator_logger = logging.getLogger("cloud_pipelines_backend.orchestrator_sql")

    # orchestrator_logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter("%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")

    # stderr_handler = logging.StreamHandler()
    # stderr_handler.setLevel(logging.INFO)
    # stderr_handler.setFormatter(formatter)

    # # TODO: Disable the default logger instead of not adding a new one
    # # orchestrator_logger.addHandler(stderr_handler)
    # logger.addHandler(stderr_handler)

    logger.info("Starting the orchestrator")

    # With autobegin=False you always need to begin a transaction, even to query the DB.
    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )

    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri=data_root_uri,
        logs_root_uri=logs_root_uri,
        default_task_annotations=default_task_annotations,
        sleep_seconds_between_queue_sweeps=sleep_seconds_between_queue_sweeps,
    )
    orchestrator.run_loop()


run_configured_orchestrator = lambda: run_orchestrator(
    db_engine=db_engine,
    storage_provider=storage_provider,
    data_root_uri=artifacts_root_uri,
    logs_root_uri=logs_root_uri,
    sleep_seconds_between_queue_sweeps=sleep_seconds_between_queue_sweeps,
)
# endregion


# region: API Server initialization
import contextlib
import threading
import traceback

import fastapi
from fastapi import staticfiles

from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import database_ops


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    database_ops.initialize_and_migrate_db(db_engine=db_engine)
    threading.Thread(
        target=run_configured_orchestrator,
        daemon=True,
    ).start()
    yield


app = fastapi.FastAPI(
    title="Cloud Pipelines API",
    version="0.0.1",
    separate_input_output_schemas=False,
    lifespan=lifespan,
)


@app.exception_handler(Exception)
def handle_error(request: fastapi.Request, exc: BaseException):
    exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return fastapi.responses.JSONResponse(
        status_code=503,
        content={"exception": exception_str},
    )


api_router.setup_routes(
    app=app,
    db_engine=db_engine,
    user_details_getter=get_user_details,
    container_launcher_for_log_streaming=launcher,
    default_component_library_owner_username=default_component_library_owner_username,
)


# Health check needed by the Web app
@app.get("/services/ping")
def health_check():
    return {}


# @app.get("/api/users/me")
# def get_current_user(
#     user_details: typing.Annotated[
#         api_router.UserDetails | None, fastapi.Depends(get_user_details)
#     ],
# ) -> api_router.UserDetails | None:
#     return user_details


# Setting up HuggingFace auth.
# if "HF_TOKEN" in os.environ:

if ENABLE_HUGGINGFACE_AUTH:
    if "OAUTH_CLIENT_SECRET" not in os.environ:
        logger.warning(
            "HuggingFace auth is enabled, but OAUTH_CLIENT_SECRET env variable is is missing."
        )
    huggingface_hub.attach_huggingface_oauth(app, route_prefix="/api/")


# Mounting the web app if the files exist
this_dir = pathlib.Path(__file__).parent
web_app_search_dirs = [
    this_dir / ".." / "pipeline-studio-app" / "build",
    this_dir / ".." / "frontend" / "build",
    this_dir / ".." / "frontend_build",
    this_dir / "pipeline-studio-app" / "build",
]
found_frontend_build_files = False
for web_app_dir in web_app_search_dirs:
    if web_app_dir.exists():
        found_frontend_build_files = True
        logger.info(
            f"Found the Web app static files at {str(web_app_dir)}. Mounting them."
        )
        # The Web app base URL is currently static and hardcoded.
        # TODO: Remove this mount once the base URL becomes relative.
        app.mount(
            "/pipeline-studio-app/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static",
        )
        app.mount(
            "/",
            staticfiles.StaticFiles(directory=web_app_dir, html=True),
            name="static",
        )
if not found_frontend_build_files:
    logger.warning("The Web app files were not found. Skipping.")
# endregion
