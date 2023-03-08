"""POC for loading config."""
from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Dict, List, Type, Union

from pydantic import Extra, Field, ValidationError, validator
from typing_extensions import Final

from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
)
from great_expectations.datasource.fluent.interfaces import (
    Datasource,  # noqa: TCH001
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    _SourceFactories,
)

if TYPE_CHECKING:
    from pydantic.error_wrappers import ErrorDict as PydanticErrorDict


logger = logging.getLogger(__name__)


_FLUENT_STYLE_DESCRIPTION: Final[str] = "Fluent Datasources"

_MISSING_FLUENT_DATASOURCES_ERRORS: Final[List[PydanticErrorDict]] = [
    {
        "loc": ("fluent_datasources",),
        "msg": "field required",
        "type": "value_error.missing",
    }
]


class GxConfig(FluentBaseModel):
    """Represents the full fluent configuration file."""

    fluent_datasources: Dict[str, Datasource] = Field(
        ..., description=_FLUENT_STYLE_DESCRIPTION
    )

    @property
    def datasources(self) -> Dict[str, Datasource]:
        return self.fluent_datasources

    class Config:
        extra = Extra.ignore  # ignore any old style config keys

    @validator("fluent_datasources", pre=True)
    @classmethod
    def _load_datasource_subtype(cls, v: Dict[str, dict]):
        logger.info(f"Loading 'datasources' ->\n{pf(v, depth=2)}")
        loaded_datasources: Dict[str, Datasource] = {}

        for ds_name, config in v.items():
            ds_type_name: str = config.get("type", "")
            if not ds_type_name:
                # TODO: (kilo59 122222) ideally this would be raised by `Datasource` validation
                # https://github.com/pydantic/pydantic/issues/734
                raise ValueError(f"'{ds_name}' is missing a 'type' entry")

            try:
                ds_type: Type[Datasource] = _SourceFactories.type_lookup[ds_type_name]
                logger.debug(f"Instantiating '{ds_name}' as {ds_type}")
            except KeyError as type_lookup_err:
                raise ValueError(
                    f"'{ds_name}' has unsupported 'type' - {type_lookup_err}"
                ) from type_lookup_err

            datasource = ds_type(**config)

            # the ephemeral asset should never be serialized
            if DEFAULT_PANDAS_DATA_ASSET_NAME in datasource.assets:
                datasource.assets.pop(DEFAULT_PANDAS_DATA_ASSET_NAME)

            # if the default pandas datasource has no assets, it should not be serialized
            if (
                datasource.name != DEFAULT_PANDAS_DATASOURCE_NAME
                or len(datasource.assets) > 0
            ):
                loaded_datasources[datasource.name] = datasource

                # TODO: move this to a different 'validator' method
                # attach the datasource to the nested assets, avoiding recursion errors
                for asset in datasource.assets.values():
                    asset._datasource = datasource

        logger.info(f"Loaded 'datasources' ->\n{repr(loaded_datasources)}")

        if v and not loaded_datasources:
            logger.info(f"Of {len(v)} entries, no 'datasources' could be loaded")
        return loaded_datasources

    @classmethod
    def parse_yaml(
        cls: Type[GxConfig], f: Union[pathlib.Path, str], _allow_empty: bool = False
    ) -> GxConfig:
        """
        Overriding base method to allow an empty/missing `fluent_datasources` field.
        Other validation errors will still result in an error.

        TODO (kilo59) 122822: remove this as soon as it's no longer needed. Such as when
        we use a new `config_version` instead of `fluent_datasources` key.
        """
        if _allow_empty:
            try:
                super().parse_yaml(f)
            except ValidationError as validation_err:
                errors_list: List[PydanticErrorDict] = validation_err.errors()
                logger.info(
                    f"{cls.__name__}.parse_yaml() failed with errors - {errors_list}"
                )
                if errors_list == _MISSING_FLUENT_DATASOURCES_ERRORS:
                    logger.info(
                        f"{cls.__name__}.parse_yaml() returning empty `fluent_datasources`"
                    )
                    return cls(fluent_datasources={})
                else:
                    logger.warning(
                        "`_allow_empty` does not prevent unrelated validation errors"
                    )
                    raise
        return super().parse_yaml(f)