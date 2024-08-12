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
