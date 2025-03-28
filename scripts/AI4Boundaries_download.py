import logging
import time
import requests

from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/DRLL/AI4BOUNDARIES/"
VALID_EXTENSIONS = (".tif", ".nc", ".csv", ".gpkg")
DOWNLOAD_TIMEOUT = 30
MAX_RETRIES = 3  # Maximum retries for failed downloads
CHUNK_SIZE = 8192  # 8 KB per chunk


def download_file(file_url: str, dst_path: Path, session: requests.Session, chunk_size: int = CHUNK_SIZE):
    """
    Download a file in chunks to dst_path.

    Args:
        file_url (str): URL of the file to download.
        dst_path (Path): File location on disk after download.
        session (requests.Session): Persistent session for requests.
        chunk_size (int): Bytes per chunk (default 8192).
    """
    try:
        with session.get(file_url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
            r.raise_for_status()
            with open(dst_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)
    except requests.RequestException as e:
        logging.warning(f"Failed to download {file_url}: {e}")
        raise  # Propagate exception for retry logic


def scrape_directory(base_url: str, visited: set, files: list, session: requests.Session):
    """
    Recursively scrape a website directory and collect valid file URLs.

    Args:
        base_url (str): URL to scrape.
        visited (set): Set of already visited URLs (to prevent re-scraping).
        files (list): List of discovered file URLs (mutates in-place).
        session (requests.Session): Persistent session for efficient requests.
    """
    if base_url in visited:
        return

    try:
        response = session.get(base_url, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.warning(f"Request failed for {base_url}: {e}")
        return

    visited.add(base_url)  # Mark URL as visited
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href", "").strip()
        if not href:
            continue

        sub_url = urljoin(base_url, href)

        if not sub_url.startswith(BASE_URL):
            continue

        if href.endswith("/"):
            scrape_directory(sub_url, visited, files, session)  # Recursive call
        elif href.lower().endswith(VALID_EXTENSIONS):
            files.append(sub_url)


def download_ai4boundaries(destination: Path):
    """
    Download the entire AI4Boundaries dataset to the destination directory.

    Args:
        destination (Path): The local directory where files will be saved.
    """
    destination.mkdir(parents=True, exist_ok=True)

    logging.info(f"Scraping data from {BASE_URL}")
    visited = set()
    file_urls = []

    with requests.Session() as session:
        scrape_directory(BASE_URL, visited, file_urls, session)

    logging.info(f"Found {len(file_urls)} files to download.")

    # Create local folder structure
    logging.info("Creating folder structure...")
    for subdir_url in visited:
        local_subdir = subdir_url.replace(BASE_URL, "").lstrip("/")
        if local_subdir:
            (destination / local_subdir).mkdir(parents=True, exist_ok=True)

    # Start file downloads
    logging.info("Starting file downloads...")
    failed_downloads = []

    with tqdm(total=len(file_urls), desc="Downloading Files", unit="file") as pbar, requests.Session() as session:
        for file_url in file_urls:
            relative_path = file_url.replace(BASE_URL, "").lstrip("/")
            local_file = destination / relative_path

            if local_file.exists():
                logging.info(f"Skipping {local_file}, already exists.")
                pbar.update(1)
                continue

            try:
                download_file(file_url, local_file, session)
            except Exception as e:
                logging.warning(f"Failed first attempt: {file_url} -> {e}")
                failed_downloads.append(file_url)

            pbar.update(1)

    # Retry logic for failed downloads
    for attempt in range(MAX_RETRIES):
        if not failed_downloads:
            break

        logging.info(f"Retrying {len(failed_downloads)} failed downloads (Attempt {attempt+1}/{MAX_RETRIES})")
        new_failed = []

        for file_url in tqdm(failed_downloads, desc=f"Retry Attempt {attempt+1}", unit="file"):
            relative_path = file_url.replace(BASE_URL, "").lstrip("/")
            local_file = destination / relative_path

            try:
                time.sleep(2)  # Brief pause before retry
                with requests.Session() as session:
                    download_file(file_url, local_file, session)
            except Exception as e:
                logging.warning(f"Still failed: {file_url} -> {e}")
                new_failed.append(file_url)

        failed_downloads = new_failed  # Update the failed list

    # Log number of files that didn't download
    if failed_downloads:
        logging.warning(f"Final failures: {len(failed_downloads)} files were not downloaded.")
        for file_url in failed_downloads:
            logging.warning(f" - {file_url}")

    logging.info("Download finished!")


if __name__ == "__main__":
    datasets_dir = Path("/home/jovyan/nfs/mgatti/datasets")
    output_path = datasets_dir / "AI4Boundaries"
    download_ai4boundaries(output_path)