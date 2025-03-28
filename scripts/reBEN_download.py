import requests
import subprocess

from pathlib import Path

# Define variables
url = "https://zenodo.org/records/10891137/files/BigEarthNet-S2.tar.zst?download=1"
datasets_dir = Path("/home/jovyan/nfs/mgatti/datasets")
output_file = datasets_dir / "BigEarthNet-S2.tar.zst"
extract_dir = datasets_dir / "BigEarthNet-S2"

if __name__ == "__main__":
    # Download the file
    print(f"Downloading {output_file} from {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    else:
        print("Download failed! Exiting.")
        exit(1)

    # Extract the file
    print(f"Extracting {output_file}...")
    Path(extract_dir).mkdir(exist_ok=True)
    result = subprocess.run(["tar", "--use-compress-program=unzstd", "-xf", output_file, "-C", extract_dir], check=False)
    if result.returncode != 0:
        print("Extraction failed! Exiting.")
        exit(1)

    # Clean up
    print("Cleaning up...")
    Path(output_file).unlink()

    print(f"Done! Files extracted to {extract_dir}.")