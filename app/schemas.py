from typing import (
    Any,
)

from pydantic import (
    BaseModel,
)


class FeatureFlag(BaseModel):
    criteria: Any | None = None
    value: bool = False
