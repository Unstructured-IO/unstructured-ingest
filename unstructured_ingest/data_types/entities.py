from pydantic import BaseModel, Field


class Entity(BaseModel):
    type: str
    entity: str


class EntityRelationship(BaseModel):
    to: str
    from_: str = Field(..., alias="from")
    relationship: str


class EntitiesData(BaseModel):
    items: list[Entity] = Field(default_factory=list)
    relationships: list[EntityRelationship] = Field(default_factory=list)
