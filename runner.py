from models import Directory, File, Element, State, HasState, expire_all_states_to
from pyarrow.hdfs import connect as hdfs_connector
from datetime import datetime
from neomodel import db

def eternity():
    return datetime(9999, 1, 1)

class HdfsToNeo4j:

    def __init__(self, import_name, directory, version):
        self._hdfs = hdfs_connector()
        self._import_name = import_name
        self._directory = directory
        self._version = datetime.fromtimestamp(version)

    @db.write_transaction
    def update(self):
        expire_all_states_to(self._version)
        self._update_directory({ 'name': self._directory })

    def _name_from(self, path):
        if path is self._directory:
            return self._import_name
        else:
            path_elements = path.split('/')
            return path_elements[-1:][0].strip('/')

    def _local_path_from(self, path):
        return path.replace(self._directory, '/' + self._import_name)

    def _directory_from(self, path):
        directory = Directory.get_or_create({
            'path': self._local_path_from(path),
            'name': self._name_from(path)
        })[0]

        directory.source = path
        return directory

    def _file_from(self, path):
        file = File.get_or_create({
            'path': self._local_path_from(path),
            'name': self._name_from(path)
        })[0]

        file.source = path
        return file

    def _update_directory(self, node):
        directory = self._directory_from(node['name'])

        for child in self._hdfs.ls(directory.source, detail=True):
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
        return self._hdfs.info(file.source)['size']

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
