import os
import logging
import threading
from ftplib import FTP, error_perm
from pathlib import Path
import tarfile


datasets_dir = Path("/home/jovyan/nfs/mgatti/datasets")
output_path = datasets_dir / "SEN12MS"

# FTP server details
ftp_host = "dataserv.ub.tum.de"
ftp_user = "m1474000"
ftp_pass = "m1474000"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def connect_to_ftp():
    """Connect to the FTP server."""
    try:
        ftp = FTP(ftp_host, timeout=30)  # Set a timeout for the connection
        ftp.login(ftp_user, ftp_pass)
        ftp.set_pasv(True)  # Enable passive mode (needed for some FTP servers)
        logging.info("Connected to FTP server.")
        return ftp
    except Exception as e:
        logging.error(f"Error connecting to FTP server: {e}")
        raise


def extract_tar_gz_file(tar_path):
    """Extract tar.gz or tgz file in a separate thread."""
    try:
        with tarfile.open(tar_path, "r:gz") as tar:  # Open as a gzipped tar file
            logging.info(f"Extracting {tar_path}...")
            tar.extractall(path=output_path)
            logging.info(f"Extraction of {tar_path} completed.")
    except Exception as e:
        logging.error(f"Failed to extract {tar_path}: {e}")


def download_files_from_ftp(ftp):
    """Download all files from the FTP server."""
    try:
        files = ftp.nlst()  # List all files in the current directory
        logging.info(f"Found {len(files)} files on the server.")

        # Ensure the local directory exists
        local_dir = Path(output_path)
        local_dir.mkdir(parents=True, exist_ok=True)

        for filename in files:
            if '_s1' in filename:
                logging.info(f"Skipping file: {filename} (contains '_s1')")
                continue
            
            local_filename = local_dir / filename
            try:
                with open(local_filename, 'wb') as local_file:
                    ftp.retrbinary(f'RETR {filename}', local_file.write)
                logging.info(f"Downloaded: {filename}")

                # If the downloaded file is a .tar.gz file, extract it in another thread
                if filename.endswith('.tar.gz') or filename.endswith('.tgz'):
                    extract_thread = threading.Thread(target=extract_tar_gz_file, args=(local_filename,))
                    extract_thread.start()

            except Exception as e:
                logging.error(f"Failed to download {filename}: {e}")

    except error_perm as e:
        logging.error(f"Permission error: {e}")
    except Exception as e:
        logging.error(f"Error during file download: {e}")


if __name__ == "__main__":
    """Main function to execute the FTP download process."""
    try:
        # Connect to the FTP server
        ftp = connect_to_ftp()

        # Download files from the FTP server
        download_files_from_ftp(ftp)

        # Close the FTP connection
        ftp.quit()
        logging.info("All files have been downloaded.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")