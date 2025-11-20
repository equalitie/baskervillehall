from sklearn.model_selection import train_test_split
import shap
from baskervillehall.feature_extractor import FeatureExtractor
import logging
import pandas as pd
import numpy as np
from sklearn.metrics import (
    average_precision_score, precision_recall_curve,
    precision_score, recall_score, f1_score, classification_report
)
import xgboost as xgb
import matplotlib.pyplot as plt


class BaskervilleClassifier(object):
    def __init__(
            self,
            n_estimators=1000,
            features=None,
            categorical_features=None,
            pca_feature=False,
            datetime_format='%Y-%m-%d %H:%M:%S',
            learning_rate=0.04,
            early_stopping_rounds=20,
            logger=None,
    ):
        super().__init__()
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.early_stopping_rounds = early_stopping_rounds

        # Default numeric features if not provided
        if features is None:
            features = [
                'request_rate', 'post_rate', 'request_interval_average',
                'request_interval_std', 'response4xx_to_request_ratio',
                'response5xx_to_request_ratio', 'top_page_to_request_ratio',
                'unique_path_rate', 'unique_path_to_request_ratio',
                'unique_query_rate', 'unique_query_to_unique_path_ratio',
                'image_to_html_ratio', 'js_to_html_ratio', 'css_to_html_ratio',
                'path_depth_average', 'path_depth_std', 'payload_size_log_average',
                'entropy',
                # 'num_requests', 'duration',
                'edge_count', 'static_ratio',
                'ua_count', 'api_ratio', 'num_ciphers', 'num_languages',
                'ua_score', 'hour_bucket', 'odd_hour', 'fingerprints_score',
                'interval_cv', 'interval_consistency',
                'rate_499'
            ]

        # Default categorical features if not provided
        if categorical_features is None:
            categorical_features = [
                # 'country',
                # 'primary_session',
                # 'bad_bot',
                # 'human',
                'cipher',
                'valid_browser_ciphers',
                'weak_cipher',
                'headless_ua',
                'bot_ua',
                'ai_bot_ua',
                'verified_bot',
                'datacenter_asn',
                'short_ua'
            ]

        # Feature extractor (numeric + categorical)
        self.feature_extractor = FeatureExtractor(
            features=features,
            categorical_features=categorical_features,
            pca_feature=pca_feature,
            datetime_format=datetime_format,
            normalize=False,
            use_onehot_encoding=False,
            logger=self.logger
        )

        # Model & training artifacts
        self.model = None
        self.best_round = None
        self.threshold = None
        self.best_f1 = None
        self.aucpr_test = None

        # CV learning curves (train vs cv)
        self.train_mean = None
        self.test_mean = None
        self.train_std = None
        self.test_std = None

        # Threshold scan curves on test
        self.thresholds = None
        self.precisions = None
        self.recalls = None

        # PR-curve on test set
        self.pr_precision = None
        self.pr_recall = None

        # SHAP explainer
        self.explainer = None

    def clear_embeddings(self):
        """Clear any PCA / embedding state in the feature extractor (if used)."""
        self.feature_extractor.clear_embeddings()

    def get_all_features(self):
        """Return the full list of feature names (numeric + categorical + optional PCA)."""
        return self.feature_extractor.get_all_features()

    def fit(self, sessions, y):
        """
        Convenience wrapper: compute features from raw sessions and then call fit_on_features.
        This is memory-heavy if 'sessions' is large. For big datasets prefer fit_on_features().
        """
        self.logger.info("Extracting features...")
        X = self.feature_extractor.fit_transform(sessions)
        return self.fit_on_features(X, y)

    def fit_on_features(self, X, y):
        """
        Train the classifier on precomputed features X (numpy array or DataFrame) and labels y.

        X:
            - numpy array of shape (n_samples, n_features) with feature order exactly matching
              self.get_all_features(), or
            - pandas DataFrame with matching columns (will be reordered if needed).
        y:
            - array-like of shape (n_samples,)
        """
        self.logger.info("Fitting Baskerville classifier...")
        self.logger.info("Preparing dataset...")
        # Train/test split (stratified because of heavy class imbalance)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=0.4,
            random_state=42,
            stratify=y
        )

        # Convert to DataFrame to keep feature names
        X_train = pd.DataFrame(X_train, columns=self.get_all_features())
        X_test = pd.DataFrame(X_test, columns=self.get_all_features())

        # Compute scale_pos_weight from train only (class imbalance handling)
        pos = (y_train == 1).sum()
        neg = (y_train == 0).sum()
        scale_pos_weight = neg / max(1, pos)

        # XGBoost params for CV and final training
        cv_params = {
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "tree_method": "hist",
            "learning_rate": self.learning_rate,
            "max_depth": 6,
            "min_child_weight": 5,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "gamma": 0.1,
            "lambda": 1.0,
            "alpha": 0.0,
            "scale_pos_weight": scale_pos_weight * 0.7,
            "max_delta_step": 1,
            "seed": 77,
        }

        # CV with early stopping to find best num_boost_round
        dtrain = xgb.DMatrix(X_train, label=y_train)
        self.logger.info("Training model...")
        cv = xgb.cv(
            params=cv_params,
            dtrain=dtrain,
            num_boost_round=self.n_estimators,
            nfold=5,
            stratified=True,
            metrics=('aucpr',),
            early_stopping_rounds=self.early_stopping_rounds,
            verbose_eval=True,
        )

        self.best_round = cv.shape[0]
        self.logger.info(f"Best num_boost_round (CV): {self.best_round}")

        # Store learning curves for later plotting
        self.train_mean = cv['train-aucpr-mean'].values
        self.test_mean = cv['test-aucpr-mean'].values
        self.train_std = cv['train-aucpr-std'].values
        self.test_std = cv['test-aucpr-std'].values

        # Train final model on full train data with best_round
        self.logger.info(f"Training final model...")
        self.model = xgb.train(
            params=cv_params,
            dtrain=dtrain,
            num_boost_round=self.best_round
        )

        # Evaluate on test set
        dtest = xgb.DMatrix(X_test)
        y_proba = self.model.predict(dtest)

        self.aucpr_test = average_precision_score(y_test, y_proba)
        self.logger.info(f"AUC-PR (test): {self.aucpr_test:.5f}")

        # Find optimal threshold by F1
        thresholds = np.linspace(0.01, 0.99, 99)
        f1s = [f1_score(y_test, (y_proba > t).astype(int), zero_division=0) for t in thresholds]
        best_idx = int(np.argmax(f1s))
        self.thresholds = thresholds
        self.threshold = thresholds[best_idx]
        self.best_f1 = f1s[best_idx]
        self.logger.info(f"Best threshold by F1: {self.threshold:.3f} | F1={self.best_f1:.3f}")

        # Log classification report at the chosen threshold
        y_pred_best = (y_proba > self.threshold).astype(int)
        self.logger.info("\nClassification report @ best threshold:")
        self.logger.info("\n" + classification_report(y_test, y_pred_best, digits=3))

        # PR-curve for test set
        pr_precision, pr_recall, _ = precision_recall_curve(y_test, y_proba)
        self.pr_precision = pr_precision
        self.pr_recall = pr_recall

        # Precision & Recall vs threshold
        self.precisions = [
            precision_score(y_test, (y_proba > t).astype(int), zero_division=0)
            for t in thresholds
        ]
        self.recalls = [
            recall_score(y_test, (y_proba > t).astype(int), zero_division=0)
            for t in thresholds
        ]

        # SHAP explainer for model interpretability
        self.explainer = shap.TreeExplainer(self.model)

    def _bot_score_from_proba(self, proba: np.ndarray) -> np.ndarray:
        """
        Convert raw P(bot) probabilities into Cloudflare-style bot scores (1..99):

          - 1   → almost certainly bot
          - 2–29 → likely automated
          - 30–99 → likely human
          - higher score → more human-like

        Uses self.threshold as the boundary between automated / human traffic.
        """
        p = proba.astype(float)
        t = float(self.threshold)

        # Default: all scores = 50, just in case threshold is degenerate
        score = np.full_like(p, 50.0, dtype=float)

        # Edge cases: if threshold is badly defined
        eps = 1e-9
        if t <= eps:
            # Threshold ≈ 0 → use simple monotonic mapping human_score = 1 - p
            score = 1.0 + (1.0 - p) * 98.0
        elif t >= 1.0 - eps:
            # Threshold ≈ 1 → almost everything is "human"
            score = 99.0 - p * 98.0
        else:
            # p <= t → map to [30..99] (likely human)
            mask_human = p <= t
            if mask_human.any():
                score[mask_human] = 30.0 + (t - p[mask_human]) * 69.0 / t

            # p > t → map to [1..29] (automated / likely automated)
            mask_bot = ~mask_human
            if mask_bot.any():
                score[mask_bot] = 1.0 + (1.0 - p[mask_bot]) * 28.0 / (1.0 - t)

        # Clip to [1, 99] and convert to integers
        score = np.clip(score, 1.0, 99.0)
        return score.astype(int)

    def transform(self, sessions, use_shapley=True):
        """
        Apply model to new sessions:
        - Extract features
        - Predict probabilities & class labels using stored threshold
        - Optionally compute SHAP values
        - Return (y_pred, shap_values, X_df)
        """
        # Feature extraction
        X_np = self.feature_extractor.transform(sessions)

        # Predictions
        dX = xgb.DMatrix(X_np, feature_names=self.get_all_features())
        y_proba = self.model.predict(dX)
        y_pred = (y_proba > self.threshold).astype(int)

        bot_score = self._bot_score_from_proba(y_proba)

        # SHAP values (can be expensive, so controlled by flag)
        if use_shapley and self.explainer is not None:
            shap_values = self.explainer.shap_values(X_np)
        else:
            shap_values = None

        # Features as DataFrame
        X_df = pd.DataFrame(X_np, columns=self.get_all_features())

        return y_pred, bot_score, shap_values, X_df

    # ===========================
    # Plotting helpers for Jupyter
    # ===========================

    def plot_learning_curves(self, ax=None, show_best_round=True):
        """
        Plot train vs cv-test AUC-PR over boosting rounds (from xgb.cv).
        """
        if self.train_mean is None or self.test_mean is None:
            raise RuntimeError("Model is not fitted yet.")

        x = np.arange(1, len(self.train_mean) + 1)

        created_fig = False
        if ax is None:
            fig, ax = plt.subplots(figsize=(8, 5))
            created_fig = True

        ax.plot(x, self.train_mean, label="train AUC-PR")
        ax.fill_between(
            x,
            self.train_mean - self.train_std,
            self.train_mean + self.train_std,
            alpha=0.15
        )
        ax.plot(x, self.test_mean, label="cv test AUC-PR")
        ax.fill_between(
            x,
            self.test_mean - self.test_std,
            self.test_mean + self.test_std,
            alpha=0.15
        )

        if show_best_round and self.best_round is not None:
            ax.axvline(self.best_round, linestyle="--", label=f"best_round={self.best_round}")

        ax.set_xlabel("Boosting round")
        ax.set_ylabel("AUC-PR")
        ax.set_title("Learning curves (train vs cv test)")
        ax.grid(True)
        ax.legend()

        if created_fig:
            plt.show()

    def plot_pr_curve(self, ax=None):
        """
        Plot Precision–Recall curve on the held-out test set.
        """
        if self.pr_precision is None or self.pr_recall is None:
            raise RuntimeError("PR curve data not available. Run fit() first.")

        created_fig = False
        if ax is None:
            fig, ax = plt.subplots(figsize=(7, 5))
            created_fig = True

        ax.plot(self.pr_recall, self.pr_precision)
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        title = "Precision–Recall curve (test)"
        if self.aucpr_test is not None:
            title += f" | AUC-PR={self.aucpr_test:.4f}"
        ax.set_title(title)
        ax.grid(True)

        if created_fig:
            plt.show()

    def plot_threshold_curves(self, ax=None):
        """
        Plot Precision, Recall and F1 as a function of classification threshold (on test set).
        """
        if self.thresholds is None or self.precisions is None or self.recalls is None:
            raise RuntimeError("Threshold curves not available. Run fit() first.")

        thresholds = np.array(self.thresholds)
        precisions = np.array(self.precisions)
        recalls = np.array(self.recalls)
        f1s = 2 * precisions * recalls / (precisions + recalls + 1e-9)

        created_fig = False
        if ax is None:
            fig, ax = plt.subplots(figsize=(8, 5))
            created_fig = True

        ax.plot(thresholds, precisions, label="Precision")
        ax.plot(thresholds, recalls, label="Recall")
        ax.plot(thresholds, f1s, label="F1")

        if self.threshold is not None:
            ax.axvline(self.threshold, linestyle="--", label=f"Best thr={self.threshold:.2f}")

        ax.set_xlabel("Threshold")
        ax.set_ylabel("Score")
        ax.set_title("Precision / Recall / F1 vs Threshold (test)")
        ax.grid(True)
        ax.legend()

        if created_fig:
            plt.show()

    # ===========================
    # Feature importance
    # ===========================

    def get_feature_importance(self, importance_type="gain", top_n=None):
        """
        Return a DataFrame with feature importance.
        importance_type: 'gain', 'weight', 'cover', 'total_gain', 'total_cover'
        """
        if self.model is None:
            raise RuntimeError("Model is not fitted yet.")

        booster = self.model  # xgb.Booster
        score = booster.get_score(importance_type=importance_type)

        df = pd.DataFrame(
            {"feature": list(score.keys()), "importance": list(score.values())}
        ).sort_values("importance", ascending=False)

        if top_n is not None:
            df = df.head(top_n)

        return df
