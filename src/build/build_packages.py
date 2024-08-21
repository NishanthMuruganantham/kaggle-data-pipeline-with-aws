"""
This module automates the build process for packaging AWS Lambda layers and handler files.

The script performs the following tasks:
1. Configures logging for tracking the build process.
2. Builds packages required for AWS Lambda layers by:
    - Creating a distribution tarball from the project setup configuration.
    - Extracting the tarball into a site-packages directory.
    - Installing necessary Python dependencies into the site-packages directory, ensuring compatibility with AWS Lambda's runtime environment.
    - Packaging the site-packages directory into a zip file suitable for deployment as a Lambda layer.
3. Zips individual AWS Lambda handler files for deployment.
4. Cleans up temporary files and directories created during the build process.

Key constants and paths are imported from the `build.constants` module.

Functions:
- `build_packages()`: Coordinates the overall package build process, including cleanup.
- `_clean_up()`: Deletes temporary files and directories.
- `_create_layer_package()`: Handles the creation of the AWS Lambda layer package, including dependency installation and zipping.
- `_run_command(command)`: Executes shell commands with error handling.
- `_zip_lambda_handler_files()`: Compresses individual Lambda handler files into zip archives.

Usage:
Call `build_packages()` to start the process of creating the necessary Lambda deployment packages.
"""

import logging
import os
import shutil
import subprocess
import zipfile
from build.constants import (
    DIST_FOLDER,
    LAMBDA_HANDLER_FILES,
    PACKAGE_NAME,
    LAYER_PATH,
    OUTPUT_FOLDER,
    REQUIREMENTS_TXT_FILE_PATH,
    SETUP_FILE_PATH,
    SITE_PACKAGES_PATH,
    TARBALL_PATH,
    TEMPORARY_PACKAGE_FOLDER,
)


# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def build_packages():
    """Builds the packages for the AWS Lambda layer."""
    logging.info("Starting package build process...")
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    try:
        _create_layer_package()
        _zip_lambda_handler_files()
        logging.info("Packages built successfully.")
    finally:
        _clean_up()

def _clean_up():
    """Cleans up temporary directories and files."""
    logging.info("Cleaning up temporary files...")
    shutil.rmtree(TEMPORARY_PACKAGE_FOLDER, ignore_errors=True)
    shutil.rmtree(LAYER_PATH, ignore_errors=True)
    shutil.rmtree(DIST_FOLDER, ignore_errors=True)
    logging.info("Cleanup completed.")

def _create_layer_package():
    """Creates the AWS Lambda layer package including compatible libraries."""
    logging.info("Creating layer package...")
    _run_command(f"python {SETUP_FILE_PATH} sdist")

    os.makedirs(SITE_PACKAGES_PATH, exist_ok=True)
    logging.info(f"Extracting {TARBALL_PATH} to {SITE_PACKAGES_PATH}")
    _run_command(f"tar -xzf {TARBALL_PATH} --strip-components=2 -C {SITE_PACKAGES_PATH}")

    # Install pandas, numpy, and lxml with specific compatible versions
    logging.info(f"Installing required dependencies from {REQUIREMENTS_TXT_FILE_PATH}")
    _run_command(
        f"pip install -r {REQUIREMENTS_TXT_FILE_PATH} -t {SITE_PACKAGES_PATH} --platform manylinux2014_x86_64 --only-binary=:all: --no-cache-dir"
    )

    # Create the final ZIP file for the Lambda layer
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

def _zip_lambda_handler_files():
    """Zips the lambda handler files."""
    logging.info("Zipping lambda handler files...")
    for file_path in LAMBDA_HANDLER_FILES:
        file_name = os.path.basename(file_path)
        zip_file_name = f"{os.path.splitext(file_name)[0]}.zip"
        zip_file_path = os.path.join(OUTPUT_FOLDER, zip_file_name)

        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path, arcname=file_name)

        logging.info(f"File '{file_name}' zipped and placed in the '{OUTPUT_FOLDER}' folder successfully.")
