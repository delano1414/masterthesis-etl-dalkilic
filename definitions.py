from dagster import Definitions, load_assets_from_modules
from . import assets as assets_mod

defs = Definitions(
    assets=load_assets_from_modules([assets_mod]),
)
