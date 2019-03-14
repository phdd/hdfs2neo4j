# neomodel_remove_labels --db bolt://neo4j:neo4j@localhost:7687
# neomodel_install_labels models.py --db bolt://neo4j:neo4j@localhost:7687

from neomodel import (StructuredNode, StructuredRel, StringProperty,
                        RelationshipTo, DateTimeProperty, IntegerProperty, db)


def expire_all_states_to(version):
    db.cypher_query("""
        MATCH ()-[has_state:HAS_STATE]-()
        WHERE has_state.until > localdatetime({version})
        SET has_state.until = {version}
        RETURN has_state;
    """, {
        'version': version
    })


class HasState(StructuredRel):

    since = StringProperty(required=True)
    until = StringProperty(required=True)


class State(StructuredNode):

    """
    original import directory root
    """
    root = StringProperty(required=True)

    size = IntegerProperty()


class Element(StructuredNode):

    """
    helper for walking the directory tree
    """
    source = ''

    path = StringProperty(required=True)
    name = StringProperty(required=True)


class Directory(Element):

    children = RelationshipTo('Element', 'CONTAINS')


class File(Element):

    state = RelationshipTo('State', 'HAS_STATE', model=HasState)


class TextFile(File):

    pass


class XMLFile(File):

    pass


class JARFile(File):

    pass


class ZIPFile(File):

    pass


class BinaryFile(File):

    pass
