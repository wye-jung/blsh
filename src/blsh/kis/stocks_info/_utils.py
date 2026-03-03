import os
import ssl
import urllib.request
import zipfile


def download_and_extract(url, base_dir):
    """Download a zip file from url, extract to base_dir, and remove the zip."""
    ssl._create_default_https_context = ssl._create_unverified_context
    zip_name = url.rsplit("/", 1)[-1]  # e.g. "kospi_code.mst.zip"
    zip_path = os.path.join(base_dir, zip_name)
    urllib.request.urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(base_dir)

    os.remove(zip_path)
