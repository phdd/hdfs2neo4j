import abc
import re

from models import *

class FileFactory(metaclass=abc.ABCMeta):

    def __init__(self, successor=None):
        self._successor = successor

    def create_file(self, properties):
        if self._successor is not None:
            return self._successor.create_file(properties)
        else:
            return File.get_or_create(properties)[0]

    def _use_other_type_for(self, properties):
        return FileFactory.create_file(self, properties)

    def _match(self, string, *patterns):
        for pattern in patterns:
            if re.search(pattern, string):
                return True

        return False


class TextFileFactory(FileFactory):

    def create_file(self, properties):
        if self._match(properties['name'], '\.properties\W*'):
            return TextFile.get_or_create(properties)[0]
        else:
            return self._use_other_type_for(properties)


class XMLFileFactory(FileFactory):

    def create_file(self, properties):
        if self._match(properties['name'], '\.xml\W*', '\.xsd\W*', '\.odx\W*'):
            return XMLFile.get_or_create(properties)[0]
        else:
            return self._use_other_type_for(properties)


class JARFileFactory(FileFactory):

    def create_file(self, properties):
        if self._match(properties['name'], '\.jar\W*'):
            return JARFile.get_or_create(properties)[0]
        else:
            return self._use_other_type_for(properties)


class ZIPFileFactory(FileFactory):

    def create_file(self, properties):
        if self._match(properties['name'], '\.zip\W*'):
            return ZIPFile.get_or_create(properties)[0]
        else:
            return self._use_other_type_for(properties)

class BinaryFileFactory(FileFactory):

    def create_file(self, properties):
        if self._match(properties['name'], '\.bin\W*', '\.dll\W*', '\.exe\W*'):
            return BinaryFile.get_or_create(properties)[0]
        else:
            return self._use_other_type_for(properties)
