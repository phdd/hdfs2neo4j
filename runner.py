from models import Directory, File, Element, State, HasState
from pyarrow.hdfs import connect as hdfs_connector
from neomodel import db

class HdfsToNeo4j: # TODO Filesystem to Neo4j (inject FS connector)

    def __init__(self, directory, version):
        self._hdfs = hdfs_connector()
        self._directory = directory
        self._version = version

    @db.write_transaction
    def update(self):
        # TODO set all HasState.valid_to now => other concern, other class! Visitor?
        self._update_directory(self._hdfs.info(self._directory))

    def _nameFrom(self, path):
        return path.split('/')[-1:][0]

    def _directory_from(self, path): # TODO get or create
        return Directory(path=path, name=self._nameFrom(path)).save()

    def _file_from(self, path): # TODO get or create
        return File(path=path, name=self._nameFrom(path)).save()

    def _update_directory(self, node):
        directory = None

        if 'path' in node:
            directory = self._directory_from(node['path'])
        else:
            directory = self._directory_from(node['name'])

        for child in self._hdfs.ls(directory.path, detail=True):
            child_element = None
            
            if child['kind'] is 'directory':
                child_element = self._update_directory(child)
            else:
                child_element = self._file_from(child['name'])
        
            directory.children.connect(child_element)

        return directory
