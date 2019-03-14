
class FileComparator(object):

    def __init__(self, hdfs):
        self._hdfs = hdfs

    def has_changed(self, last_state, file):
        return last_state.size != self._hdfs.info(file.source)['size']
