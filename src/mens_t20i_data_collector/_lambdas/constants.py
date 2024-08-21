"""
This module defines constants used for downloading and managing CricSheet data.

Constants:
- `CRICSHEET_DATA_DOWNLOADING_URL`: The URL from which the T20 men's cricket JSON data can be downloaded.
- `CRICSHEET_DATA_S3_FOLDER_NAME`: The name of the S3 folder where the downloaded CricSheet data is stored.
- `CRICSHEET_DATA_S3_FOLDER_TO_STORE_NEW_JSON_FILES_ZIP`: The S3 folder where new JSON files, downloaded from CricSheet, are stored as a zip.
- `CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP`: The S3 folder where processed JSON files are 
        stored as a zip after being handled by the application.

These constants are primarily used in the context of an AWS-based workflow that involves downloading, storing, and processing CricSheet data.
"""

CRICSHEET_DATA_DOWNLOADING_URL: str = "https://cricsheet.org/downloads/t20s_male_json.zip"
CRICSHEET_DATA_S3_FOLDER_NAME: str = "cricsheet_data"
CRICSHEET_DATA_S3_FOLDER_TO_STORE_NEW_JSON_FILES_ZIP: str = "new_cricsheet_data"
CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP: str = "processed_data"
