import dataclasses


@dataclasses.dataclass
class Domain:
    """Domain data structure for loaders"""
    name: str
    url: str | None
    source: str
    category: str


@dataclasses.dataclass
class Source:
    """Source data structure"""
    url: str
    category: str
    category_source: str
    getter_def: str | None
    mapper_def: str | None


@dataclasses.dataclass
class MongoDomainMetadata:
    _id: str
    category: str

    @staticmethod
    def from_domain(domain: Domain):
        return MongoDomainMetadata(_id=domain.name, category=domain.category)

    def to_dict(self):
        return dataclasses.asdict(self)
