import logging
from collections.abc import AsyncGenerator
from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
)
from typing import (
    Any,
    cast,
)

import nats
from config import Settings
from fastapi import (
    FastAPI,
    Request,
)
from schemas import (
    FeatureFlag,
)
from services.dynconf.service import (
    Dynconf,
    DynconfManager,
)

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[dict[str, Any], Any]:
    """Defines the lifespan of the FastAPI application.

    This context manager handles setup and teardown of resources like the NATS connection,
    JetStream, and dynamic configuration management.

    Args:
        _: FastAPI: The FastAPI application instance (unused).

    Yields:
        Dict[str, Any]: A dictionary containing the `features` configuration which can be accessed during the application lifespan.
    """
    settings = Settings()

    async with AsyncExitStack() as stack:
        # Establish a connection to NATS
        nc = await nats.connect(servers=str(settings.nats_servers))
        await stack.enter_async_context(nc)

        # Create JetStream context
        jetstream = nc.jetstream()

        # Initialize the dynamic configuration manager
        manager = await stack.enter_async_context(
            DynconfManager(js=jetstream, catch_validation_exceptions=True),
        )

        # Load the "features" configuration bucket
        features = await manager.new(name="features", model=FeatureFlag)

        yield {"features": features}


app = FastAPI(
    title="dynconf-nats",
    summary="A simple dynconf impl over NATS KeyValue",
    version="0.1.0",
    lifespan=lifespan,
    contact={
        "name": "Artem Ukolov",
        "url": "https://t.me/deusdeveloper",
        "email": "deusdeveloper@icloud.com",
    },
    license_info={
        "name": "MIT",
        "identifier": "MIT",
    },
)


@app.put("/feature")
async def put_feature(request: Request, feature: str, value: FeatureFlag) -> None:
    """Endpoint to add or overwrite a feature flag configuration.

    This endpoint stores a new feature flag in the dynamic configuration, overwriting any existing value for the same key.

    Args:
        request (Request): The incoming HTTP request.
        feature (str): The name of the feature flag to set.
        value (FeatureFlag): The feature flag configuration data.
    """
    features = cast(Dynconf[FeatureFlag], request.state.features)
    await features.aset(key=feature, value=value)


@app.post("/feature")
async def post_feature(request: Request, feature: str, value: FeatureFlag) -> None:
    """Endpoint to update an existing feature flag configuration.

    This endpoint updates a feature flag's value if it matches the expected revision, ensuring no unintentional overwrites occur.

    Args:
        request (Request): The incoming HTTP request.
        feature (str): The name of the feature flag to update.
        value (FeatureFlag): The updated feature flag configuration data.
    """
    features = cast(Dynconf[FeatureFlag], request.state.features)
    await features.aupdate(key=feature, value=value)


@app.get("/feature")
async def get_feature(
    request: Request,
    feature: str,
    force: bool = False,
) -> FeatureFlag | None:
    """Endpoint to retrieve a feature flag configuration.

    This endpoint returns the value of a feature flag. If `force` is True, it performs a fresh fetch from the store;
    otherwise, it returns the locally cached value.

    Args:
        request (Request): The incoming HTTP request.
        feature (str): The name of the feature flag to retrieve.
        force (bool, optional): If True, fetches the latest value directly from the store. Defaults to False.

    Returns:
        Optional[FeatureFlag]: The feature flag configuration, or None if it is not found.
    """
    features = cast(Dynconf[FeatureFlag], request.state.features)
    if force:
        return await features.aget(feature)
    return features.get(feature)
