import abc
from pandas import DataFrame


class Transformation(abc.ABC):
    @abc.abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("transform method must be implemented in the subclass")
