import rasterio
import torch
import xarray as xr

import pandas as pd
from pathlib import Path
from torch.utils.data import Dataset


class AI4BoundariesDataset(Dataset):
    def __init__(self, root_dir, csv_file):
        """
        Args:
            root_dir (str): Root directory of the dataset (contains 'orthophoto' and 'sentinel2' folders).
            csv_file (str): Path to the CSV file that contains the columns ['area', 'file_id'].
        """
        self.root_dir = Path(root_dir)
        self.csv_file = csv_file
        
        df = pd.read_csv(self.csv_file)    
        self.entries = df.to_dict(orient="records")
        
        # Build a list of (ortho_path, sentinel_path, ortho_mask_path, sentinel_mask_path) from CSV
        self.pairs = self._build_pairs()
    
    def __len__(self):
        return len(self.pairs)

    def __getitem__(self, idx):
        ortho_path, sentinel_path, ortho_mask_path, sentinel_mask_path = self.pairs[idx]

        # --- Ortho (.tif) ---
        with rasterio.open(ortho_path) as src:
            ortho_data = torch.tensor(src.read(), dtype=torch.float32)

        # --- Ortho Mask (.tif) ---
        with rasterio.open(ortho_mask_path) as src:
            ortho_mask = torch.tensor(src.read(), dtype=torch.float32)

        # --- Sentinel (.nc) ---
        sentinel_data = xr.open_dataset(sentinel_path)
        bands = ["B2", "B3", "B4", "B8", "NDVI"]
        
        sentinel_images = torch.tensor(
            sentinel_data[bands].to_array().transpose("time", "variable", "y", "x").values,
            dtype=torch.float32
        )
        
        # --- Sentinel Mask (.tif) ---
        with rasterio.open(sentinel_mask_path) as src:
            sentinel_mask = torch.tensor(src.read(), dtype=torch.float32)

        # Mask is 4-channels: extent mask, boundary mask, distance mask, enumeration mask
        return {
            "ortho": ortho_data,
            "ortho_mask": ortho_mask,
            "sentinel": sentinel_images * 1e-4,
            "sentinel_mask": sentinel_mask
        }
    
    def _build_pairs(self):
        """
        Build the list of (ortho_path, sentinel_path, ortho_mask_path, sentinel_mask_path)
        tuples by reading the area and file_id from the CSV entries.
        """
        all_pairs = []

        for entry in self.entries:
            file_id = entry["image"].split("_")[1]
            area = entry["country"]
            
            ortho_name = f"{area}_{file_id}_ortho_1m_512.tif"
            ortho_mask_name = f"{area}_{file_id}_ortholabel_1m_512.tif"
            sentinel_name = f"{area}_{file_id}_S2_10m_256.nc"
            sentinel_mask_name = f"{area}_{file_id}_S2label_10m_256.tif"

            # Construct the full paths
            ortho_path = self.root_dir / "orthophoto" / "images" / area / ortho_name
            ortho_mask_path = self.root_dir / "orthophoto" / "masks" / area / ortho_mask_name
            sentinel_path = self.root_dir / "sentinel2" / "images" / area / sentinel_name
            sentinel_mask_path = self.root_dir / "sentinel2" / "masks" / area / sentinel_mask_name

            all_pairs.append((ortho_path, sentinel_path, ortho_mask_path, sentinel_mask_path))

        return all_pairs