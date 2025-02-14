import torch
import torch.nn as nn
from monai.networks.nets import SwinUNETR


class Sentinel2SwinUNETR3D(nn.Module):
    def __init__(self, in_channels, feature_size=48):
        """
        Swin UNETR 3D for Instance Segmentation
        Args:
            in_channels (int): Number of input channels (e.g., 4 bands).
            feature_size (int): Size of features for Swin UNETR backbone.
        """
        super(Sentinel2SwinUNETR3D, self).__init__()
        
        # Swin UNETR backbone
        self.swin_unetr = SwinUNETR(
            img_size=(6, 256, 256),  # Input size: (Depth, Height, Width)
            in_channels=in_channels,
            out_channels=2,  # 2 outputs: Foreground Probability + Embedding Map
            feature_size=feature_size,
            depths=[2, 2, 4, 2],  # Depth of Swin Transformer stages
            num_heads=[3, 6, 12, 24],  # Number of attention heads per stage
            norm_name="instance",  # Normalization type
            spatial_dims=3,  # 3D inputs
        )
        self.sigmoid = nn.Sigmoid()  # For foreground probability
        self.tanh = nn.Tanh()  # For embedding map (optional)

    def forward(self, x):
        """
        Forward pass for instance segmentation.
        Args:
            x (torch.Tensor): Input tensor of shape (Batch, Channels, Depth, Height, Width).
        Returns:
            torch.Tensor: Foreground probabilities and embedding maps.
        """
        # Forward pass through Swin UNETR
        output = self.swin_unetr(x)
        
        # Split output into foreground probability and embeddings
        foreground_prob = self.sigmoid(output[:, 0:1])  # First channel: Foreground probabilities
        embedding_map = self.tanh(output[:, 1:])        # Remaining channels: Embedding map
        return foreground_prob, embedding_map