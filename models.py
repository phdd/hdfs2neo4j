# neomodel_remove_labels --db bolt://neo4j:neo4j@localhost:7687
# neomodel_install_labels models.py --db bolt://neo4j:neo4j@localhost:7687

from neomodel import (StructuredNode, StructuredRel, StringProperty,
                        RelationshipTo, DateTimeProperty)


class HasState(StructuredRel):

    valid_from = DateTimeProperty(db_property='from')
    valid_to = DateTimeProperty(db_property='to')


class State(StructuredNode):

    pass


class Element(StructuredNode):

    created = DateTimeProperty(default_now=True)

    path = StringProperty(unique_index=True, required=True)
    name = StringProperty()

    state = RelationshipTo('State', 'HAS', model=HasState)


class Directory(Element):

    children = RelationshipTo('Element', 'CONTAINS')


class File(Element):

    pass
