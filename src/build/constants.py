from pathlib import Path


# Constants
DIST_FOLDER = Path("dist")
LAMBDA_HANDLER_FILES = [
    'src/mens_t20i_data_collector/lambdas/download_from_cricsheet/download_from_cricsheet_lambda_function.py',
]
PACKAGE_NAME = "mens_t20i_data_collector"
PYTHON_VERSION = "3.11"
LAYER_PATH = Path("layer")
OUTPUT_FOLDER = Path("output")
SETUP_FILE_PATH = Path("setup.py")
SITE_PACKAGES_PATH = LAYER_PATH / f"python/lib/python{PYTHON_VERSION}/site-packages"
TARBALL_PATH = DIST_FOLDER / f"{PACKAGE_NAME}-0.1.tar.gz"
TEMPORARY_PACKAGE_FOLDER = Path("temporary_package")
