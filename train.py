import torch

from dataset.ai4boundaries import AI4BoundariesDataset
from pathlib import Path

root_dir = Path("/jovyan/home/nfs/mgatti/datasets/AI4Boundaries")

train_csv = Path("/jovyan/home/nfs/mgatti/datasets/AI4Boundaries/sentinel2/train.csv")
val_csv = Path("/jovyan/home/nfs/mgatti/datasets/AI4Boundaries/sentinel2/val.csv")
test_csv = Path("/jovyan/home/nfs/mgatti/datasets/AI4Boundaries/sentinel2/test.csv")

train_dataset = AI4BoundariesDataset(root_dir, train_csv)
val_dataset = AI4BoundariesDataset(root_dir, val_csv)
test_dataset = AI4BoundariesDataset(root_dir, test_csv)
