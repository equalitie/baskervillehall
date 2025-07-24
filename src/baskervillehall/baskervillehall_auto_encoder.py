import logging
import shap
import pandas as pd
import torch.nn as nn
import numpy as np
from sklearn.preprocessing import StandardScaler
from torch.utils.data import TensorDataset, random_split, DataLoader
import torch
import torch.nn.functional as F
from sklearn.cluster import KMeans

from baskervillehall.feature_extractor import FeatureExtractor


class EarlyStopping:
    def __init__(self, patience=5, min_delta=0.0):
        self.patience = patience
        self.min_delta = min_delta
        self.counter = 0
        self.best_loss = None
        self.early_stop = False

    def step(self, val_loss):
        if self.best_loss is None:
            self.best_loss = val_loss
            return False

        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.early_stop = True
        return self.early_stop


class AutoEncoder(nn.Module):
    def __init__(self, input_dim=33, latent_dim=8, dropout_p=0.2):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 24),
            nn.LayerNorm(24),
            nn.ReLU(),
            nn.Dropout(p=dropout_p),
            nn.Linear(24, 16),
            nn.LayerNorm(16),
            nn.ReLU(),
            nn.Dropout(p=dropout_p),
            nn.Linear(16, latent_dim)  # Bottleneck
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 16),
            nn.LayerNorm(16),
            nn.ReLU(),
            nn.Dropout(p=dropout_p),
            nn.Linear(16, 24),
            nn.LayerNorm(24),
            nn.ReLU(),
            nn.Dropout(p=dropout_p),
            nn.Linear(24, input_dim)  # Output
        )

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)


class BaskervillehallAutoEncoder(object):

    def __init__(
            self,
            latent_dim=4,
            contamination=0.05,
            num_epochs=50,
            features=None,
            categorical_features=None,
            pca_feature=False,
            datetime_format='%Y-%m-%d %H:%M:%S',
            shap_num_background=30,
            logger=None
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.latent_dim = latent_dim
        self.feature_extractor = FeatureExtractor(
            features=features,
            categorical_features=categorical_features,
            pca_feature=pca_feature,
            datetime_format=datetime_format,
            use_onehot_encoding=False,
            logger=self.logger
        )

        self.contamination = contamination
        self.model = None
        self.scaler = None
        self.num_epochs = num_epochs
        self.threshold = None
        self.explainer = None
        self.background_data = None
        self.shap_num_background = shap_num_background

    def clear_embeddings(self):
        self.feature_extractor.clear_embeddings()

    def set_contamination(self, contamination):
        self.contamination = contamination

    def get_all_features(self):
        return self.feature_extractor.get_all_features()

    def _score_fn(self, x_np: np.ndarray) -> np.ndarray:
        device = torch.device("cpu")
        self.model.to(device).eval()
        x_t = torch.from_numpy(x_np.astype(np.float32)).to(device)
        with torch.no_grad():
            recon = self.model(x_t)
            # average MSE per feature
            mse = torch.mean((recon - x_t) ** 2, dim=1)
        return mse.cpu().numpy()

    def fit(self, sessions):
        # 1. Extract features and scale them
        vectors = np.array(self.feature_extractor.fit_transform(sessions))
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(vectors)  # shape (N, n_features)

        # 2. Convert to torch.Tensor
        X_tensor = torch.from_numpy(X_scaled.astype(np.float32))

        # 3. Prepare datasets and loaders
        batch_size = 64
        val_split = 0.2
        dataset = TensorDataset(X_tensor)
        val_size = int(len(dataset) * val_split)
        train_size = len(dataset) - val_size
        train_dataset, val_dataset = random_split(dataset, [train_size, val_size])
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_dataset, batch_size=batch_size)

        # 4. Initialize model, optimizer with L2 reg, loss fn, early stopping
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = AutoEncoder(
            input_dim=X_tensor.shape[1], latent_dim=self.latent_dim, dropout_p=0.5
        ).to(device)
        optimizer = torch.optim.Adam(
            self.model.parameters(), lr=1e-3, weight_decay=1e-4
        )  # L2 regularization
        loss_fn = nn.MSELoss(reduction='mean')  # average MSE
        early_stopping = EarlyStopping(patience=3, min_delta=0.003)

        # 5. Training
        train_losses, val_losses = [], []
        for epoch in range(self.num_epochs):
            self.model.train()
            total_train_loss = 0
            for (batch_x,) in train_loader:
                batch_x = batch_x.to(device)
                optimizer.zero_grad()
                recon = self.model(batch_x)
                loss = loss_fn(recon, batch_x)
                loss.backward()
                optimizer.step()
                total_train_loss += loss.item() * batch_x.size(0)

            self.model.eval()
            total_val_loss = 0
            with torch.no_grad():
                for (batch_x,) in val_loader:
                    batch_x = batch_x.to(device)
                    recon = self.model(batch_x)
                    total_val_loss += loss_fn(recon, batch_x).item() * batch_x.size(0)

            avg_train = total_train_loss / train_size
            avg_val = total_val_loss / val_size
            train_losses.append(avg_train)
            val_losses.append(avg_val)
            self.logger.info(f"Epoch {epoch+1}: Train={avg_train:.6f} | Val={avg_val:.6f}")

            if early_stopping.step(avg_val):
                self.logger.info("Early stopping triggered.")
                break

        # 6. Threshold on full set
        self.model.eval()
        with torch.no_grad():
            X_tensor = X_tensor.to(device)
            recon = self.model(X_tensor)
            errors = F.mse_loss(recon, X_tensor, reduction='none')
            per_sample_error = errors.mean(dim=1)
        self.threshold = torch.quantile(per_sample_error, 1.0 - self.contamination).item()

        # 7. SHAP background and explainer
        self.background_data = KMeans(
            n_clusters=self.shap_num_background
        ).fit(X_scaled).cluster_centers_
        self.explainer = shap.Explainer(
            self._score_fn, self.background_data, algorithm="permutation"
        )
        self.scaler = scaler
        return self

    def transform(self, sessions, use_shapley=True):
        self.model.eval().to('cpu')
        f_vectors = np.array(self.feature_extractor.transform(sessions))
        df_features = pd.DataFrame(f_vectors)
        X_scaled = self.scaler.transform(df_features.values)

        with torch.no_grad():
            X_t = torch.tensor(X_scaled, dtype=torch.float32)
            recon = self.model(X_t)
            # average MSE per feature
            reconstruction_errors = F.mse_loss(recon, X_t, reduction='none')
            anomaly_scores = reconstruction_errors.mean(dim=1).cpu().numpy()

        if self.explainer is None:
            if self.background_data is None:
                raise RuntimeError("self.background_data is missing")
            self.explainer = shap.Explainer(
                self._score_fn, self.background_data, algorithm="permutation"
            )

        shap_values = self.explainer(X_scaled, max_evals=300)
        return anomaly_scores, shap_values
