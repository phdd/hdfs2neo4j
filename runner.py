from pyarrow.hdfs import connect as hdfs_connector
from neomodel import db

from models import *
from factories import *
from utils import FileComparator

def eternity():
    return "9999-01-01T00:00:00"

class HdfsToNeo4j:

    def __init__(self, import_name, directory, version):
        self._hdfs = hdfs_connector()
        self._comparator = FileComparator(self._hdfs)

        self._import_name = import_name
        self._directory = directory.rstrip('/')
        self._version = version

        self._fileFactory = XMLFileFactory(
                            ZIPFileFactory(
                            JARFileFactory(
                            TextFileFactory(
                            BinaryFileFactory(
                            FileFactory())))))

    @db.write_transaction
    def update(self):
        expire_all_states_to(self._import_name, self._version)
        self._update_directory({ 'name': self._directory })

    def _name_from(self, path):
        if path is self._directory:
            return self._import_name
        else:
            path_elements = path.split('/')
            return path_elements[-1:][0].strip('/')

    def _local_path_from(self, path):
        return path.replace(self._directory, '')

    def _directory_from(self, path):
        directory = Directory.get_or_create({
            'path': self._local_path_from(path),
            'name': self._name_from(path)
        })[0]

        directory.source = path
        return directory

    def _file_from(self, path):
        file = self._fileFactory.create_file({
            'path': self._local_path_from(path),
            'name': self._name_from(path)
        })

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

    def _create_new_state_for(self, file):
        state = State(
            size=self._hdfs.info(file.source)['size'],
            root=self._directory
        ).save()

        file.state.connect(state, { 'since': self._version, 'until': eternity() })

    def _last_state_of(self, file):
        try:
            return file.state.match(until=self._version)[0]
        except IndexError:
            return None

    def _update_state_of(self, file):
        last_state = self._last_state_of(file)

        if last_state: # file exists already
            last_state_rel = file.state.relationship(last_state)

            if self._comparator.has_changed(last_state, file):
                self._create_new_state_for(file)
                last_state_rel.until = self._version
            else:
                last_state_rel.until = eternity()

            last_state_rel.save()

        else: # file has been created
            self._create_new_state_for(file)
