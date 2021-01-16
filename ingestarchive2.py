import subprocess
from internetarchive import download

archivedir = "archiveteam-twitter-stream-2019-06"
#download(archivedir, verbose=True)

subprocess.run(["/home/k3sc0re/repos/scrum03/ingest_archive", archivedir])