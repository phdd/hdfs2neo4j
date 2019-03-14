import subprocess
 
 
def execute(args):
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = process.communicate()
    code =  process.returncode
    return code, output, error


def checksum_for(path):
    code, checksum, error = execute(['hdfs', 'dfs', '-checksum', path])

    if code is not 0:
        raise ValueError("cannot get checksum for '%s'" % (path,))
    
    return checksum.decode('utf8').split('\t')[-1]
