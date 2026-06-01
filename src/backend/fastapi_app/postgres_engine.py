import logging
import os
from typing import Optional

from azure.identity import AzureDeveloperCliCredential
from pgvector.asyncpg import register_vector
from sqlalchemy import event
from sqlalchemy.engine import AdaptedConnection, URL
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from fastapi_app.dependencies import get_azure_credential

logger = logging.getLogger("ragapp")

POSTGRES_AAD_SCOPE = "https://ossrdbms-aad.database.windows.net/.default"


def _get_password_from_azure_credential(azure_credential) -> str:
    """
    AzureDeveloperCliCredential (azure.identity) is synchronous.
    Its get_token() returns an AccessToken immediately.
    """
    token = azure_credential.get_token(POSTGRES_AAD_SCOPE)
    return token.token


async def create_postgres_engine(
    *,
    host: str,
    username: str,
    database: str,
    password: Optional[str],
    sslmode: Optional[str],
    azure_credential,
) -> AsyncEngine:
    token_based_password = False

    if host.endswith(".database.azure.com"):
        token_based_password = True
        logger.info("Authenticating to Azure Database for PostgreSQL using Azure Identity...")
        if azure_credential is None:
            raise ValueError("Azure credential must be provided for Azure Database for PostgreSQL")
        password = _get_password_from_azure_credential(azure_credential)
    else:
        logger.info("Authenticating to PostgreSQL using password...")
        if password is None:
            raise ValueError("POSTGRES_PASSWORD must be set for non-Azure PostgreSQL hosts")

    # Build a safe SQLAlchemy URL so passwords/tokens are properly URL-encoded.
    query = {}
    if sslmode:
        # This repo uses "?ssl=<value>" for asyncpg; preserve that behavior.
        query["ssl"] = sslmode

    db_url = URL.create(
        drivername="postgresql+asyncpg",
        username=username,
        password=password,
        host=host,
        database=database,
        query=query or None,
    )

    engine = create_async_engine(db_url, echo=False)

    @event.listens_for(engine.sync_engine, "connect")
    def register_custom_types(dbapi_connection: AdaptedConnection, *args):
        logger.info("Registering pgvector extension...")
        try:
            dbapi_connection.run_async(register_vector)
        except ValueError:
            logger.warning("Could not register pgvector data type yet as vector extension has not been CREATEd")

    @event.listens_for(engine.sync_engine, "do_connect")
    def update_password_token(dialect, conn_rec, cargs, cparams):
        """
        SQLAlchemy 'do_connect' is a synchronous event.
        Do NOT call loop.run_until_complete() here (causes 'event loop already running').
        Instead, fetch a new token synchronously (AzureDeveloperCliCredential is sync).
        """
        if token_based_password:
            logger.info("Updating password token for Azure Database for PostgreSQL")
            cparams["password"] = _get_password_from_azure_credential(azure_credential)

    return engine


async def create_postgres_engine_from_env(azure_credential=None) -> AsyncEngine:
    if azure_credential is None and os.environ["POSTGRES_HOST"].endswith(".database.azure.com"):
        azure_credential = get_azure_credential()

    return await create_postgres_engine(
        host=os.environ["POSTGRES_HOST"],
        username=os.environ["POSTGRES_USERNAME"],
        database=os.environ["POSTGRES_DATABASE"],
        password=os.environ.get("POSTGRES_PASSWORD"),
        sslmode=os.environ.get("POSTGRES_SSL"),
        azure_credential=azure_credential,
    )


async def create_postgres_engine_from_args(args, azure_credential=None) -> AsyncEngine:
    if azure_credential is None and args.host.endswith(".database.azure.com"):
        if tenant_id := args.tenant_id:
            logger.info("Authenticating to Azure using Azure Developer CLI Credential for tenant %s", tenant_id)
            azure_credential = AzureDeveloperCliCredential(tenant_id=tenant_id, process_timeout=60)
        else:
            logger.info("Authenticating to Azure using Azure Developer CLI Credential")
            azure_credential = AzureDeveloperCliCredential(process_timeout=60)

    return await create_postgres_engine(
        host=args.host,
        username=args.username,
        database=args.database,
        password=args.password,
        sslmode=args.sslmode,
        azure_credential=azure_credential,
    )
