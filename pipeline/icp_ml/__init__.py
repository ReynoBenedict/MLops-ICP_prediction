"""ICP forecasting ML pipeline: data validation, feature engineering, training, registry, forecasting.

This package has no web-framework dependencies. It is used by the offline CLI (``icp-ml``),
by the backend job runner and by the backend inference path, so that training and serving
share exactly one implementation of the feature logic.
"""

__version__ = "2.0.0"
