"""base_transformation.py: Defines the base class for feature extraction transformations."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import abc
from typing import Mapping

from pandas import DataFrame


class Transformation(abc.ABC):
    def __new__(cls, *args, **kwargs):
        # This enables transformations not to explicitly define a constructor that accepts
        # the configuration object, while others can define it.
        if cls.__init__ is object.__init__:
            def new_init(_self, _config):
                pass

            cls.__init__ = new_init

        return super().__new__(cls)

    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("transform method must be implemented in the subclass")

    @abc.abstractmethod
    def get_new_column_names(self) -> Mapping[str, str]:
        raise NotImplementedError("get_new_column_names method must be implemented in the subclass")
