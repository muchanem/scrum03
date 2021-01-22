import subprocess
from internetarchive import download
from shutil import rmtree
archivedir = ["archiveteam-twitter-stream-2019-01","archiveteam-twitter-stream-2019-02","archiveteam-twitter-stream-2019-03","archiveteam-twitter-stream-2019-04","archiveteam-twitter-stream-2019-05"]
for archive in archivedir:
    download(archive, verbose=True)
    subprocess.run(["/root/ingest_archive", archive])
    rmtree(("/root/ingest_archive/" + archive))