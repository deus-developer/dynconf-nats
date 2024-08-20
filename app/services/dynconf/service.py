import asyncio
import logging
from collections.abc import Iterator, Mapping
from contextlib import AsyncExitStack
from typing import (
    TypeVar,
    overload,
)

import nats.errors
import nats.js.errors
from nats.js import JetStreamContext
from nats.js.kv import KeyValue
from pydantic import (
    BaseModel,
    ValidationError,
)

ValueType = TypeVar("ValueType", bound=BaseModel)
ValueDefaultType = TypeVar("ValueDefaultType")


class DynconfManager:
    def __init__(
        self,
        js: JetStreamContext,
        catch_validation_exceptions: bool = False,
    ) -> None:
        """Initializes the DynconfManager for managing dynamic configurations.

        Args:
            js (JetStreamContext): The JetStream context to be used for accessing Key-Value stores.
            catch_validation_exceptions (bool, optional): If True, validation errors will be logged but not raised.
        """
        self.js = js
        self.catch_validation_exceptions = catch_validation_exceptions
        self.stack = AsyncExitStack()

    async def new(self, name: str, model: type[ValueType]) -> "Dynconf[ValueType]":
        """Creates a new Dynconf instance to manage a specific Key-Value bucket.

        Args:
            name (str): The name of the Key-Value bucket.
            model (Type[ValueType]): The Pydantic model class used for validating stored configurations.

        Returns:
            Dynconf[ValueType]: An instance of Dynconf configured for the specified bucket and model.
        """
        key_value = await self.js.key_value(bucket=name)
        watcher = await key_value.watchall(include_history=True)

        dynconf = Dynconf(
            name=name,
            js=self.js,
            key_value=key_value,
            watcher=watcher,
            model=model,
            catch_validation_exceptions=self.catch_validation_exceptions,
        )
        self.stack.push_async_callback(dynconf.aclose)

        return dynconf

    async def aclose(self) -> None:
        """Closes all managed resources, including Dynconf instances and Key-Value watchers."""
        await self.stack.aclose()

    async def __aenter__(self) -> "DynconfManager":
        """Enters the async context, allowing the DynconfManager to be used with `async with` syntax.

        Returns:
            DynconfManager: The current instance.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exits the async context, ensuring all resources are properly closed.

        Args:
            exc_type: The type of exception (if any) raised within the context.
            exc_val: The value of the exception (if any).
            exc_tb: The traceback object (if any).
        """
        await self.aclose()


class Dynconf(Mapping[str, ValueType]):
    def __init__(
        self,
        name: str,
        js: JetStreamContext,
        key_value: KeyValue,
        watcher: KeyValue.KeyWatcher,
        model: type[ValueType],
        catch_validation_exceptions: bool,
    ) -> None:
        """Initializes a Dynconf instance for managing dynamic configurations within a specific Key-Value bucket.

        Args:
            name (str): The name of the Key-Value bucket.
            js (JetStreamContext): The JetStream context used for Key-Value operations.
            key_value (KeyValue): The Key-Value store instance.
            watcher (KeyValue.KeyWatcher): The watcher for observing changes in the bucket.
            model (Type[ValueType]): The Pydantic model class used for validating configurations.
            catch_validation_exceptions (bool): If True, validation errors are logged instead of raising exceptions.
        """
        self._name = name
        self._js = js
        self._key_value = key_value
        self._watcher = watcher
        self._model = model
        self._catch_validation_exceptions = catch_validation_exceptions

        self._task = asyncio.create_task(self._subscriber())
        self._storage: dict[str, ValueType] = {}

    def __getitem__(self, __key: str) -> ValueType:
        """Retrieves a configuration by its key.

        Args:
            __key (str): The key of the configuration to retrieve.

        Returns:
            ValueType: The configuration associated with the key.
        """
        return self._storage[__key]

    def __len__(self) -> int:
        """Returns the number of configurations currently stored.

        Returns:
            int: The number of configurations.
        """
        return len(self._storage)

    def __iter__(self) -> Iterator[str]:
        """Returns an iterator over the keys of the stored configurations.

        Returns:
            Iterator[str]: An iterator of the configuration keys.
        """
        return iter(self._storage)

    async def aset(self, key: str, value: ValueType) -> None:
        """Sets a new value for the specified key in the Key-Value store.

        If the key already exists, this method will overwrite the existing value without checking any version or revision.

        Args:
            key (str): The key under which the configuration will be stored.
            value (ValueType): The configuration data to be stored, validated by the Pydantic model.
        """
        payload = value.model_dump_json(by_alias=True)
        await self._key_value.put(key=key, value=payload.encode("utf-8"))

    async def aupdate(self, key: str, value: ValueType) -> None:
        """Updates the value for the specified key in the Key-Value store with version checking.

        Unlike `aset`, this method ensures that the key's current revision matches the expected revision before applying the update.
        This prevents overwriting changes that might have been made since the last fetch.

        Args:
            key (str): The key of the configuration to update.
            value (ValueType): The new configuration data, validated by the Pydantic model.
        """
        payload = value.model_dump_json(by_alias=True)
        await self._key_value.update(key=key, value=payload.encode("utf-8"))

    @overload
    async def aget(self, __key: str) -> ValueType | None: ...

    @overload
    async def aget(
        self,
        __key: str,
        default: ValueDefaultType,
    ) -> ValueType | ValueDefaultType: ...

    async def aget(
        self,
        __key: str,
        default: ValueDefaultType | None = None,
    ) -> ValueType | ValueDefaultType | None:
        """Retrieves a configuration by its key with optional default value handling.

        Args:
            __key (str): The key of the configuration to retrieve.
            default (Optional[ValueDefaultType], optional): The default value to return if the configuration is not found.

        Returns:
            Union[ValueType, ValueDefaultType, None]: The configuration data if found, otherwise the default value or None.
        """
        try:
            entry = await self._key_value.get(key=__key)
        except nats.js.errors.KeyNotFoundError:
            return default

        if entry.value is None:
            return default

        try:
            value = self._model.model_validate_json(entry.value)
        except ValidationError as exc:
            if self._catch_validation_exceptions:
                logging.exception(
                    "Dynconf(name=%s) entry(key=%s) validation error",
                    self._name,
                    entry.key,
                    exc_info=exc,
                )

            return default

        self._storage[entry.key] = value
        return value

    async def _subscriber(self) -> None:
        """Internal task that listens for changes in the Key-Value store and updates the local configuration cache."""
        async for entry in self._watcher:  # type: KeyValue.Entry
            if entry.operation is None or entry.operation == "PUT":
                if entry.value is None:
                    self._storage.pop(entry.key, None)
                    continue

                try:
                    self._storage[entry.key] = self._model.model_validate_json(
                        entry.value,
                    )
                except ValidationError as exc:
                    if self._catch_validation_exceptions:
                        logging.exception(
                            "Dynconf(name=%s) entry(key=%s) validation error",
                            self._name,
                            entry.key,
                            exc_info=exc,
                        )
            elif entry.operation == "DELETE" or entry.operation == "PURGE":
                self._storage.pop(entry.key, None)

    async def aclose(self) -> None:
        """Closes the configuration watcher and stops listening for updates."""
        await self._watcher.stop()
