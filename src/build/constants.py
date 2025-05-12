"""
File to maintain the constants which are used in build_packages
"""


from pathlib import Path

# Constants
DIST_FOLDER = Path("dist")
LAMBDA_HANDLER_FILES = [
    'src/mens_t20i_data_collector/_lambdas/convert_mongodb_data_to_csv/convert_mongo_db_data_to_csv_lambda.py',
    'src/mens_t20i_data_collector/_lambdas/download_from_cricsheet/download_from_cricsheet_lambda_function.py',
    'src/mens_t20i_data_collector/_lambdas/extract_deliverywise_cricsheet_data/extract_deliverywise_cricsheet_data_lambda_function.py',
    'src/mens_t20i_data_collector/_lambdas/extract_matchwise_cricsheet_data/extract_matchwise_cricsheet_data_lambda_function.py',
    'src/mens_t20i_data_collector/_lambdas/upload_dataset_to_kaggle/upload_dataset_to_kaggle_lambda.py',
]
PACKAGE_NAME = "mens_t20i_data_collector"
PYTHON_VERSION = "3.11"
LAYER_PATH = Path("layer")
OUTPUT_FOLDER = Path("output")
REQUIREMENTS_TXT_FILE_PATH = Path("requirements.txt")
SETUP_FILE_PATH = Path("setup.py")
SITE_PACKAGES_PATH = LAYER_PATH / f"python/lib/python{PYTHON_VERSION}/site-packages"
TARBALL_PATH = DIST_FOLDER / f"{PACKAGE_NAME}-0.1.tar.gz"
TEMPORARY_PACKAGE_FOLDER = Path("temporary_package")
