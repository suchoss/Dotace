import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve


def download_file(path, url):
    os.makedirs(path, exist_ok=True)
    filename = os.path.split(urlparse(url).path)[-1].rstrip("\n")
    local_path = os.path.join(path, filename)
    if not os.path.isfile(local_path):
        logging.info("Nemam %s (%s) lokalne, stahuju", filename, url)
        urlretrieve(url, local_path)
    logging.info("Soubor %s je stažen", filename)
    return True