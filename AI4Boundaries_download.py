from ai4boundaries import download_ai4boundaries
from pathlib import Path

datasets_dir = Path("/home/jovyan/nfs/mgatti/datasets")
output_path = datasets_dir / "AI4Boundaries"

download_ai4boundaries(output_path.as_posix())