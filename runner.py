from models import Directory, File, Element, State, HasState, expire_all_states_to
from pyarrow.hdfs import connect as hdfs_connector
from datetime import datetime
from neomodel import db

def eternity():
    return datetime(9999, 1, 1)

class HdfsToNeo4j:

    def __init__(self, directory, version):
        self._hdfs = hdfs_connector()
        self._directory = directory
        self._version = datetime.fromtimestamp(version)

    @db.write_transaction
    def update(self):
        expire_all_states_to(self._version)
        self._update_directory({ 'name': self._directory })

    def _name_from(self, path):
        path_elements = path.split('/')

        if path_elements[-1:][0]: # handle trailing slashes
            return path_elements[-1:][0]
        else:
            return path_elements[-2:][0]

    def _directory_from(self, path):
        return Directory.get_or_create({
            'path': path,
            'name': self._name_from(path)
        })[0]

    def _file_from(self, path):
        return File.get_or_create({
            'path': path,
            'name': self._name_from(path)
        })[0]

    def _update_directory(self, node):
        directory = self._directory_from(node['name'])

        for child in self._hdfs.ls(directory.path, detail=True):
            child_element = None
            
            if child['kind'] is 'directory':
                child_element = self._update_directory(child)
            else:
                child_element = self._file_from(child['name'])
                self._update_state_of(child_element)

            child_element.save()
            directory.children.connect(child_element)

        return directory

    def _size_of(self, file):
        return self._hdfs.info(file.path)['size']

    def _create_new_state_for(self, file):
        state = State(size=self._size_of(file)).save()
        file.state.connect(state, { 'since': self._version, 'until': eternity() })

    """
    State comparison should be based on file checksum
    """
    def _file_has_changed(self, last_state, file):
        return last_state.size != self._size_of(file)

    def _last_state_of(self, file):
        try:
            return file.state.match(until=self._version)[0]
        except IndexError:
            return None

    def _update_state_of(self, file):
        last_state = self._last_state_of(file)

        if last_state: # file exists already
            last_state_rel = file.state.relationship(last_state)

            if self._file_has_changed(last_state, file):
                self._create_new_state_for(file)
                last_state_rel.until = self._version
            else:
                last_state_rel.until = eternity()

            last_state_rel.save()

        else: # file has been created
            self._create_new_state_for(file)
