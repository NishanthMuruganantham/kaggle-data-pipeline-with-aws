import logging
import os
from pathlib import Path
import shutil
import subprocess


# Constants
PACKAGE_NAME = "mens_t20i_data_collector"
PYTHON_VERSION = "3.11"
LAYER_PATH = Path("layer")
OUTPUT_FOLDER = Path("output")
SETUP_FILE_PATH = Path("setup.py")
SITE_PACKAGES_PATH = LAYER_PATH / f"python/lib/python{PYTHON_VERSION}/site-packages"
TARBALL_PATH = Path(f"dist/{PACKAGE_NAME}-0.1.tar.gz")
TEMPORARY_PACKAGE_FOLDER = Path("temporary_package")

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def build_packages():
    """Builds the packages for the AWS Lambda layer."""
    logging.info("Starting package build process...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    try:
        _create_layer_package()
        logging.info("Packages built successfully.")
    finally:
        _clean_up()

def _clean_up():
    """Cleans up temporary directories and files."""
    logging.info("Cleaning up temporary files...")
    shutil.rmtree(TEMPORARY_PACKAGE_FOLDER, ignore_errors=True)
    shutil.rmtree(LAYER_PATH, ignore_errors=True)
    logging.info("Cleanup completed.")

def _create_layer_package():
    """Creates the AWS Lambda layer package."""
    logging.info("Creating layer package...")
    _run_command(f"python {SETUP_FILE_PATH} sdist")

    os.makedirs(SITE_PACKAGES_PATH, exist_ok=True)
    logging.info(f"Extracting {TARBALL_PATH} to {SITE_PACKAGES_PATH}")
    _run_command(f"tar -xzf {TARBALL_PATH} --strip-components=2 -C {SITE_PACKAGES_PATH}")

    destination_zip = OUTPUT_FOLDER / f"{PACKAGE_NAME}.zip"
    logging.info(f"Zipping contents of {LAYER_PATH} to {destination_zip}")
    shutil.make_archive(base_name=destination_zip.stem, format='zip', root_dir=LAYER_PATH)
    shutil.move(f"{destination_zip.stem}.zip", destination_zip)
    logging.info("Layer package created successfully.")

def _run_command(command):
    """Runs a shell command and handles errors."""
    logging.info(f"Running command: {command}")
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed: {e}")
        raise
