# neomodel_remove_labels --db bolt://neo4j:neo4j@localhost:7687
# neomodel_install_labels models.py --db bolt://neo4j:neo4j@localhost:7687

from neomodel import (StructuredNode, StructuredRel, StringProperty,
                        RelationshipTo, DateTimeProperty, IntegerProperty, db)

from datetime import datetime


def expire_all_states_to(version):
    db.cypher_query("""
        MATCH ()-[s:HAS_STATE]-()
        WHERE s.until > {version}
        SET s.until = {version}
        RETURN s;
    """, {
        'version': (version - datetime(1970, 1, 1)).total_seconds()
    })


class HasState(StructuredRel):

    since = DateTimeProperty()
    until = DateTimeProperty()


class State(StructuredNode):

    size = IntegerProperty()


class Element(StructuredNode):

    path = StringProperty(unique_index=True, required=True)
    name = StringProperty()


class Directory(Element):

    children = RelationshipTo('Element', 'CONTAINS')


class File(Element):

    state = RelationshipTo('State', 'HAS_STATE', model=HasState)
