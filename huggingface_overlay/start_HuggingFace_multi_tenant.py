import contextlib
from collections import abc
import dataclasses
import json
import logging
import os
import pathlib
import threading
import traceback
import typing
from typing import Any


import fastapi
import fastapi.routing
from fastapi import staticfiles
import huggingface_hub
import huggingface_hub.errors
import sqlalchemy
from sqlalchemy import orm


from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_interfaces,
)

# from cloud_pipelines_backend import api_router_multi_tenant
# from cloud_pipelines_backend import api_router_multi_tenant as api_router
from cloud_pipelines_backend import api_router
from cloud_pipelines_backend import database_ops
from cloud_pipelines_backend import orchestrator_sql
from cloud_pipelines_backend.launchers import huggingface_launchers
from cloud_pipelines_backend.launchers import local_docker_launchers
from cloud_pipelines_backend.launchers import interfaces as launcher_interfaces

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
            # "level": "INFO",
            "level": "DEBUG",
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
        __name__: {
            "level": "DEBUG",
            "handlers": ["default"],
            "propagate": False,
        },
        "cloud_pipelines_backend.orchestrator_sql": {
            "level": "DEBUG",
            "handlers": ["default"],
            "propagate": False,
        },
        "cloud_pipelines_backend.launchers.huggingface_launchers": {
            "level": "DEBUG",
            "handlers": ["default"],
            "propagate": False,
        },
        "cloud_pipelines.orchestration.launchers.local_docker_launchers": {
            "level": "DEBUG",
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
            # "level": "DEBUG",
            "level": "INFO",  # Skip successful GET requests. Does not work...
            "handlers": ["default"],
        },
        "watchfiles.main": {
            "level": "WARNING",
            "handlers": ["default"],
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)



# We want to reduce noise in the HTTP access logs by removing records for successful HTTP GET requests (status code 200)
class FilterOutSuccessfulHttpGetRequests(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Uvicorn access logs store the status code in record.args[2] (as a string)
        # print(f"{record.args=}, {record=}")
        # record.args:
        # IP, HTTP method, URL, HTTP version, HTTP status code (int)
        try:
            # if record.args[1] == "GET" and 200 <= int(record.args[4]) < 300:
            if record.args[1] == "GET" and int(record.args[4]) in (200, 304):
                record.levelname = "DEBUG"
                # We have to filter out the message here since the dict config fails to filter out the DEBUG messages.
                return False
        except:
            pass
        return True


# Add the filter to the "uvicorn.access" logger
logging.getLogger("uvicorn.access").addFilter(FilterOutSuccessfulHttpGetRequests())


logger = logging.getLogger(__name__)
# endregion

ENABLE_HUGGINGFACE_AUTH = True

MULTI_TENANT_SPACE_IDS = [
    "TangleML/tangle",
    "TangleML/tangle_multi_tenant",
    "Ark-kun/tangle_multi_tenant",
]

hf_space_id = os.environ.get("SPACE_ID")
is_hf_space = bool(hf_space_id)
is_multi_tenant = (
    os.environ.get("MULTI_TENANT", "false").lower() == "true"
    or hf_space_id in MULTI_TENANT_SPACE_IDS
)
use_local_debug = not is_hf_space
# use_local_launcher = use_local_debug
use_local_launcher = False

if use_local_debug:
    is_multi_tenant = True

logger.info(f"{hf_space_id=}")
logger.info(f"{is_hf_space=}")
logger.info(f"{is_multi_tenant=}")
logger.info(f"{use_local_debug=}")
logger.info(f"{use_local_launcher=}")


# region Paths configuration

if is_hf_space:
    root_data_dir = "/data/tangle/data_multi_tenant"
else:
    root_data_dir = "./data_multi_tenant/"

root_data_dir_path_obj = pathlib.Path(root_data_dir).resolve()
root_data_dir_path = str(root_data_dir_path_obj)
logger.info(f"{root_data_dir_path=}")

root_data_dir_path_obj.mkdir(parents=True, exist_ok=True)
# endregion

# region: DB Configuration
tenant_dir_template_path_obj = root_data_dir_path_obj / "tenants" / "{tenant_id}"
database_path_template = str(tenant_dir_template_path_obj / "db.sqlite")
tenants_database_path_obj = root_data_dir_path_obj / "tenants.sqlite"
tenants_database_path = str(tenants_database_path_obj)
tenants_database_uri = f"sqlite:///{tenants_database_path}"

logger.info(f"{database_path_template=}")
logger.info(f"{tenants_database_uri=}")

tenants_database_path_obj.parent.mkdir(parents=True, exist_ok=True)
# endregion

# region: Orchestrator configuration
default_task_annotations: dict[str, str] = {}
sleep_seconds_between_queue_sweeps: float = 5.0
# endregion

# region: Authentication configuration

logger.info("os.environ=" + json.dumps(dict(os.environ), indent=2, sort_keys=True))

if "HF_TOKEN" in os.environ and is_multi_tenant:
    logger.warning("Warning: Multi-tenant spaces should not have HF_TOKEN set.")


# ! This module is executed during server startup. We do not know any tenants at this point.
# So, we can almost nothing (cannot check/create repo, cannot construct launcher etc).


@dataclasses.dataclass
class TenantIdNameToken:
    id: str
    namespace: str
    token: str


# ! parse_huggingface_oauth handles local (non-Space) auth - returned info corresponding to the local HF_TOKEN.
def get_tenant_info_for_active_user_or_die(
    request: fastapi.Request,
) -> TenantIdNameToken:
    oauth_info = huggingface_hub.parse_huggingface_oauth(request)
    if not oauth_info:
        # No -- TODO: Maybe return the demo tenant info?
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Unauthenticated user",
        )
    # logger.debug(f"get_tenant_info_for_active_user_or_die: {oauth_info=}")
    tenant_info = TenantIdNameToken(
        id=oauth_info.user_info.sub,
        namespace=oauth_info.user_info.preferred_username,
        token=oauth_info.access_token,
    )
    # logger.debug(f"get_tenant_info_for_active_user_or_die: {tenant_info=}")
    return tenant_info


# We're multi-tenant, but single-user (single user per tenant).
# So the user is always an admin of it's own tenant.
# TODO: Create multi-user mode.
# TODO: Enable demo tenants that are readable to anyone.
def get_user_details(request: fastapi.Request):
    oauth_info = huggingface_hub.parse_huggingface_oauth(request)
    logger.debug(f"get_user_details: {oauth_info=}")
    if oauth_info:
        # We're multi-tenant, but single-user (single user per tenant).
        # So the user is always an admin of it's own tenant.
        # TODO: Create multi-user mode.
        # TODO: Enable demo tenants that are readable to anyone.
        user_details = api_router.UserDetails(
            name=oauth_info.user_info.preferred_username,
            permissions=api_router.Permissions(
                read=True,
                write=True,
                admin=True,
            ),
        )
        logger.debug(f"get_user_details: {user_details=}")
        return user_details
    # FIX: ???!!!
    # Redirect to login?

    # Return unauthenticated
    return api_router.UserDetails(
        # name="anonymous",
        name=None,
        permissions=api_router.Permissions(
            read=False,
            write=False,
            admin=False,
        ),
    )


# TODO: Switch to async-supporting locks
db_engines_lock = threading.Lock()
db_engines: dict[str, sqlalchemy.Engine] = {}


def get_db_engine_for_tenant(tenant_id: str) -> sqlalchemy.Engine:
    db_engine = db_engines.get(tenant_id)
    if db_engine:
        return db_engine
    with db_engines_lock:
        # Double-checked locking
        db_engine = db_engines.get(tenant_id)
        if db_engine:
            return db_engine
        database_path = database_path_template.format(tenant_id=tenant_id)
        database_uri = "sqlite:///" + database_path
        pathlib.Path(database_path).parent.mkdir(parents=True, exist_ok=True)

        # TODO: Implement "create DB on first write" optimization
        db_engine = database_ops.create_db_engine_and_migrate_db(
            database_uri=database_uri
        )
        db_engines[tenant_id] = db_engine
    return db_engine


def get_db_engine_for_unauthenticated() -> sqlalchemy.Engine:
    return database_ops.create_db_engine_and_migrate_db(
        database_uri="sqlite:///:memory:"
    )


def get_session_factory_for_active_user(
    request: fastapi.Request,
) -> typing.Callable[[], orm.Session]:
    try:
        tenant_info = get_tenant_info_for_active_user_or_die(request=request)
        db_engine = get_db_engine_for_tenant(tenant_id=tenant_info.id)
    except:
        logger.debug(
            f"get_session_factory_for_active_user: User is unauthenticated. Returning ephemeral in-memory DB engine."
        )
        db_engine = get_db_engine_for_unauthenticated()
    return orm.sessionmaker(bind=db_engine, autoflush=False)


def get_session_generator_for_active_user(
    request: fastapi.Request,
) -> abc.Iterator[orm.Session]:
    session_factory = get_session_factory_for_active_user(request=request)
    with session_factory() as session:
        yield session


def get_launcher_for_tenant(
    tenant_id: str, tenant_namespace: str, tenant_token: str
) -> launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer]:
    del tenant_id
    if use_local_launcher:
        launcher = local_docker_launchers.DockerContainerLauncher()
    else:
        launcher = huggingface_launchers.HuggingFaceJobsContainerLauncher(
            namespace=tenant_namespace,
            hf_token=tenant_token,
            hf_job_token=tenant_token,
        )
    return launcher


def get_launcher_for_active_user(
    request: fastapi.Request,
) -> launcher_interfaces.ContainerTaskLauncher[launcher_interfaces.LaunchedContainer]:
    tenant_info = get_tenant_info_for_active_user_or_die(request=request)
    return get_launcher_for_tenant(
        tenant_id=tenant_info.id,
        tenant_namespace=tenant_info.namespace,
        tenant_token=tenant_info.token,
    )


@dataclasses.dataclass(kw_only=True)
class OrchestratorInfo:
    tenant_id: str
    tenant_namespace: str
    tenant_token: str
    artifacts_root_uri: str
    logs_root_uri: str
    launcher: launcher_interfaces.ContainerTaskLauncher[
        launcher_interfaces.LaunchedContainer
    ]
    storage_provider: storage_interfaces.StorageProvider
    orchestrator: orchestrator_sql.OrchestratorService_Sql
    orchestrator_thread: threading.Thread


def start_orchestrator_for_tenant(
    tenant_id: str,
    tenant_namespace: str,
    tenant_token: str,
    update_tenants_db: bool = True,
) -> OrchestratorInfo:
    logger.info(f"tenant={tenant_namespace}({tenant_id}): Preparing the orchestrator")
    launcher = get_launcher_for_tenant(
        tenant_id=tenant_id,
        tenant_namespace=tenant_namespace,
        tenant_token=tenant_token,
    )

    db_engine = get_db_engine_for_tenant(tenant_id=tenant_id)

    if use_local_launcher:
        artifacts_root_uri = str(tenant_dir_template_path_obj / "artifacts").format(
            tenant_id=tenant_id
        )
        logs_root_uri = str(tenant_dir_template_path_obj / "logs").format(
            tenant_id=tenant_id
        )

        from cloud_pipelines.orchestration.storage_providers import local_storage

        storage_provider = local_storage.LocalStorageProvider()
    else:
        #  Create artifact repo if it does not exist.
        artifacts_repo_id = f"{tenant_namespace}/tangle_data"
        # Do not pollute repos with debug data
        if use_local_debug:
            artifacts_repo_id += "_test"
        ensure_artifact_repo_exists(
            artifacts_repo_id=artifacts_repo_id, token=tenant_token
        )
        artifacts_root_uri = f"hf://datasets/{artifacts_repo_id}/data"
        logs_root_uri = artifacts_root_uri

        from cloud_pipelines_backend.storage_providers import huggingface_repo_storage

        # ! Need to pass proper token here!
        hf_client = huggingface_hub.HfApi(token=tenant_token)
        storage_provider = huggingface_repo_storage.HuggingFaceRepoStorageProvider(
            client=hf_client
        )

    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )

    # With autobegin=False you always need to begin a transaction, even to query the DB.
    session_factory = orm.sessionmaker(
        autocommit=False, autoflush=False, bind=db_engine
    )
    orchestrator = orchestrator_sql.OrchestratorService_Sql(
        session_factory=session_factory,
        launcher=launcher,
        storage_provider=storage_provider,
        data_root_uri=artifacts_root_uri,
        logs_root_uri=logs_root_uri,
        default_task_annotations=default_task_annotations,
        sleep_seconds_between_queue_sweeps=sleep_seconds_between_queue_sweeps,
    )
    logger.info(f"tenant={tenant_namespace}({tenant_id}): Starting the orchestrator")
    orchestrator_thread = threading.Thread(
        target=orchestrator.run_loop,
        daemon=True,
    )
    orchestrator_thread.start()

    if update_tenants_db:
        # Recording the orchestrator_info in the tenants DB
        with orm.Session(bind=tenants_db_engine) as session:
            tenant_row = session.get(TenantRow, tenant_id)
            if tenant_row:
                tenant_row.orchestrator_active = True
                launcher_class = type(launcher)
                storage_provider_class = type(storage_provider)
                launcher_class_name = (
                    f"{launcher_class.__module__}.{launcher_class.__qualname__}"
                )
                storage_provider_class_name = f"{storage_provider_class.__module__}.{storage_provider_class.__qualname__}"
                tenant_row.orchestrator_config = dict(
                    storage_provider_class_name=storage_provider_class_name,
                    artifacts_root_uri=artifacts_root_uri,
                    logs_root_uri=logs_root_uri,
                    launcher_class_name=launcher_class_name,
                )
                session.commit()
            else:
                logging.critical(
                    f"start_orchestrator_for_tenant: Started the orchestrator for {tenant_id=}, but tenants DB has no such tenant."
                )

    return OrchestratorInfo(
        tenant_id=tenant_id,
        tenant_namespace=tenant_namespace,
        tenant_token=tenant_token,
        artifacts_root_uri=artifacts_root_uri,
        logs_root_uri=logs_root_uri,
        launcher=launcher,
        storage_provider=storage_provider,
        orchestrator=orchestrator,
        orchestrator_thread=orchestrator_thread,
    )


# TODO: Switch to async-supporting locks
orchestrators_lock = threading.Lock()
orchestrators: dict[str, OrchestratorInfo] = {}


def get_or_start_orchestrator(
    tenant_id: str, tenant_namespace: str, tenant_token: str
) -> OrchestratorInfo:
    orchestrator_info = orchestrators.get(tenant_id)
    if orchestrator_info:
        return orchestrator_info
    with orchestrators_lock:
        # Double-checked locking
        orchestrator_info = orchestrators.get(tenant_id)
        if orchestrator_info:
            return orchestrator_info
        orchestrator_info = start_orchestrator_for_tenant(
            tenant_id=tenant_id,
            tenant_namespace=tenant_namespace,
            tenant_token=tenant_token,
        )
        orchestrators[tenant_id] = orchestrator_info
        return orchestrator_info


def get_or_start_orchestrator_for_active_user(
    request: fastapi.Request,
) -> OrchestratorInfo:
    tenant_info = get_tenant_info_for_active_user_or_die(request=request)
    orchestrator_info = get_or_start_orchestrator(
        tenant_id=tenant_info.id,
        tenant_namespace=tenant_info.namespace,
        tenant_token=tenant_info.token,
    )
    with orm.Session(bind=tenants_db_engine) as session:
        tenant_row = session.get(TenantRow, tenant_info.id)
        if not tenant_row:
            tenant_row = update_tenant_info_in_db(request=request)
        tenant_row.orchestrator_active = True
        session.commit()

    return orchestrator_info


def ensure_artifact_repo_exists(artifacts_repo_id: str, token: str):
    hf_client = huggingface_hub.HfApi(token=token)
    repo_type = "dataset"
    repo_exists = False
    try:
        _ = hf_client.repo_info(
            repo_id=artifacts_repo_id,
            repo_type=repo_type,
        )
        repo_exists = True
        logger.debug(
            f"ensure_artifact_repo_exists: Artifact repo exists: {artifacts_repo_id}"
        )

    except huggingface_hub.errors.RepositoryNotFoundError:
        pass
    except Exception as ex:
        raise RuntimeError(
            f"Error checking for the artifacts repo existence. {artifacts_repo_id=}"
        ) from ex
    if not repo_exists:
        logger.info(
            f"ensure_artifact_repo_exists: Artifact repo does not exist. Creating it: {artifacts_repo_id}"
        )
        try:
            _ = hf_client.create_repo(
                repo_id=artifacts_repo_id,
                repo_type=repo_type,
                private=True,
                exist_ok=True,
            )
        except Exception as ex:
            raise RuntimeError(
                f"Error creating the artifacts repo. {artifacts_repo_id=}"
            ) from ex


def do_stuff_for_tenant(tenant_id: str, tenant_namespace: str):
    del tenant_id
    del tenant_namespace
    # Don't initialize library for HuggingFace users. The initialization might require re-design.
    # # The default library must be initialized here, not when adding the Component Library routes.
    # # Otherwise the tables won't yet exist when initialization is performed.
    # from cloud_pipelines_backend import component_library_api_server as components_api
    # component_library_service = components_api.ComponentLibraryService()
    # db_engine = get_db_engine_for_tenant(tenant_id=tenant_id)
    # with orm.Session(bind=db_engine) as session:
    #     component_library_service._initialize_empty_default_library_if_missing(
    #         session=session,
    #         published_by=tenant_namespace,
    #     )
    pass


from sqlalchemy.ext import mutable


class _TenantTableBase(orm.MappedAsDataclass, orm.DeclarativeBase, kw_only=True):
    # Not really needed due to kw_only=True
    _: dataclasses.KW_ONLY

    # The mutable.MutableDict.as_mutable construct ensures that changes to dictionaries are picked up.
    # This is very important when making changes to `extra_data` dictionaries.
    type_annotation_map = {
        dict: mutable.MutableDict.as_mutable(sqlalchemy.JSON),
        list: mutable.MutableList.as_mutable(sqlalchemy.JSON),
        dict[str, Any]: mutable.MutableDict.as_mutable(sqlalchemy.JSON),
        str: sqlalchemy.String(255),
    }


class TenantRow(_TenantTableBase):
    __tablename__ = "tenant"

    id: orm.Mapped[str] = orm.mapped_column(primary_key=True)
    name: orm.Mapped[str]
    access_token: orm.Mapped[str]
    oauth_info: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    orchestrator_active: orm.Mapped[bool] = orm.mapped_column(default=False, index=True)
    orchestrator_config: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        default=None
    )
    # user_info: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)
    extra_data: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(default=None)


tenants_db_engine = sqlalchemy.create_engine(url=tenants_database_uri)


def init_tenants_db():
    # tenants_db_engine = sqlalchemy.create_engine(url=tenants_database_uri)
    TenantRow.__table__.create(tenants_db_engine, checkfirst=True)


def update_tenant_info_in_db(request: fastapi.Request) -> TenantRow:
    oauth_info: dict[str, Any] = request.session.get("oauth_info")
    if not oauth_info:
        raise ValueError(
            f"update_tenant_info_in_db: request.session does not have oauth_info."
        )
    logger.debug(f"update_tenant_info_in_db: {oauth_info=}")
    oauth_user_info: dict[str, Any] = oauth_info["userinfo"]
    token: str = oauth_info["access_token"]
    huggingface_user_info: dict[str, Any] = huggingface_hub.whoami(token=token)
    # In local mode, `oauth_user_info["sub"]`` is always "0123456789"
    # We could get the correct ID from `huggingface_user_info["id"]`
    # but huggingface_user_info is not available from the session cookie, so there would be discrepancies between IDs.
    # So, we use `oauth_user_info["sub"]`
    id = oauth_user_info["sub"]
    # id = huggingface_user_info["id"]
    logger.debug(f"update_tenant_info_in_db: {huggingface_user_info=}")
    with orm.Session(bind=tenants_db_engine, expire_on_commit=False) as session:
        # session.merge seems to delete unspecified info (like orchestrator_config) at some circumstances
        # result_row = session.merge(tenant_row)
        tenant_row = session.get(TenantRow, id)
        if tenant_row:
            tenant_row.name = oauth_user_info["preferred_username"]
            tenant_row.access_token = oauth_info["access_token"]
        else:
            tenant_row = TenantRow(
                id=id,
                name=oauth_user_info["preferred_username"],
                access_token=oauth_info["access_token"],
            )
            session.add(tenant_row)
        tenant_row.oauth_info = oauth_info
        extra_data = tenant_row.extra_data or {}
        extra_data["huggingface_user_info"] = huggingface_user_info
        tenant_row.extra_data = dict(extra_data)

        session.commit()
        session.expunge(tenant_row)
        return tenant_row


def start_all_active_tenant_orchestrators():
    logger.debug(f"start_all_active_tenant_orchestrators")
    with orm.Session(bind=tenants_db_engine) as session:
        for tenant_row in session.scalars(
            sqlalchemy.select(TenantRow).where(TenantRow.orchestrator_active)
        ):
            # TODO: Respect the orchestrator_config
            _ = get_or_start_orchestrator(
                tenant_id=tenant_row.id,
                tenant_namespace=tenant_row.name,
                tenant_token=tenant_row.access_token,
            )


# region: API Server initialization
@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI):
    init_tenants_db()
    start_all_active_tenant_orchestrators()
    yield
    if tenants_db_engine:
        tenants_db_engine.dispose()


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


def handle_pipeline_run_creation(request: fastapi.Request):
    # Do nothing before PipelineRun is created
    yield
    # Wake up the orchestrator after the PipelineRun is created
    _ = get_or_start_orchestrator_for_active_user(request=request)


api_router._setup_routes_internal(
    app=app,
    get_session=get_session_generator_for_active_user,
    user_details_getter=get_user_details,
    # TODO: Add
    # container_launcher_for_log_streaming=launcher,
    # TODO: Handle the default library
    # default_component_library_owner_username=default_component_library_owner_username,
    pipeline_run_creation_hook=handle_pipeline_run_creation,
)


# Health check needed by the Web app
@app.get("/services/ping")
def health_check():
    return {}


if ENABLE_HUGGINGFACE_AUTH:
    if "OAUTH_CLIENT_SECRET" not in os.environ:
        logger.warning(
            "HuggingFace auth is enabled, but OAUTH_CLIENT_SECRET env variable is is missing."
        )
    huggingface_hub.attach_huggingface_oauth(app, route_prefix="/api/")

    # Hook the login callback route to write info to the tenants DB
    # The route is created by huggingface_hub.attach_huggingface_oauth, so we cannot easily control it.
    auth_callback_route_candidates = [
        route
        for route in typing.cast(list[fastapi.routing.APIRoute], app.routes)
        if route.path.endswith("/oauth/huggingface/callback")
    ]
    if len(auth_callback_route_candidates) != 1:
        raise ValueError(f"{auth_callback_route_candidates=}")
    if auth_callback_route_candidates:
        auth_callback_route = auth_callback_route_candidates[0]
        auth_callback_original = auth_callback_route.endpoint
        assert auth_callback_route.dependant.call == auth_callback_route.endpoint

        def wrapped_auth_callback(*args, **kwargs) -> Any:
            # logger.debug(f"wrapped_auth_callback: {args=}, {kwargs=}")
            result = auth_callback_original(*args, **kwargs)
            request: fastapi.Request = kwargs.get("request") or args[0]
            if "oauth_info" in request.session:
                update_tenant_info_in_db(request=request)
            return result

        # The `ApiRoute.dependant.call` is the function being called, not the ApiRoute.endpoint.
        # auth_callback_route.endpoint = wrapped_auth_callback
        auth_callback_route.dependant.call = wrapped_auth_callback


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

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
