import logging
import boto3
import zipfile
import os
import io
import json
from typing import Dict
import pandas as pd
from io import StringIO
from mens_t20i_data_collector._lambdas.constants import OUTPUT_FOLDER_NAME, CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DeliverywiseCricsheetDataExtractionHandler:

    def __init__(self):
        self._s3_bucket_name = os.getenv("DOWNLOAD_BUCKET_NAME")
        self._s3_client = boto3.client("s3")
        self._temp_folder: str = "/tmp"
        self._temporary_extraction_folder: str = f"{self._temp_folder}/extracted_files"
        self._deliveries_dataframe: pd.DataFrame = pd.DataFrame(
            columns=[
                "match_id", "innings_number", "batting_team", "bowling_team", "over_number", "ball_number", "batter", "bowler", "non_striker", "wide_runs",
                "leg_bye_runs", "bye_runs", "no_ball_runs", "penalty_runs", "batsman_runs", "extra_runs", "total_runs", "player_dismissed", "dismissal_type", "fielder_name"
            ]
        )
        self._deliveries_csv_file_key: str = f"{OUTPUT_FOLDER_NAME}/deliverywise_data.csv"
        self._existing_deliveries_dataframe: pd.DataFrame = self._load_existing_deliveries_dataframe()
        self._s3_folder_to_store_processed_json_files_zip: str = CRICSHEET_DATA_S3_FOLDER_TO_STORE_PROCESSED_JSON_FILES_ZIP

        if not self._s3_bucket_name:
            logger.error("Environment variable 'DOWNLOAD_BUCKET_NAME' is missing.")
            raise ValueError("Missing required environment variable: 'DOWNLOAD_BUCKET_NAME'")

    def extract_deliverywise_cricsheet_data(self, event: dict) -> None:
        s3_event_zip_file_key = event["Records"][0]["s3"]["object"]["key"]
        logger.info(f"Zip file key from s3 event: {s3_event_zip_file_key}")
        self._extract_the_unprocessed_json_files_from_s3_event_zip_file(s3_event_zip_file_key)
        json_files_to_be_processed = self._list_all_the_json_files_in_the_extraction_folder()
        logger.info(f"JSON files to be processed: {json_files_to_be_processed}")
        for json_file in json_files_to_be_processed:
            self._process_single_json_file(json_file)
        self._update_deliveries_data_csv_file_with_newly_collected_data()
        self._upload_processed_json_files_to_processed_folder_in_s3(json_files_to_be_processed)

    def _extract_the_unprocessed_json_files_from_s3_event_zip_file(self, zip_file_key: str) -> None:
        logger.info(f"Extracting zip file: {zip_file_key}")
        unprocessed_zip_file = self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=zip_file_key)
        bytes_buffer = io.BytesIO(unprocessed_zip_file["Body"].read())
        zip_file_data = zipfile.ZipFile(bytes_buffer)
        zip_file_data.extractall(path=self._temporary_extraction_folder)
        logger.info(f"Extracted files to {self._temporary_extraction_folder}")

    def _get_delivery_data_of_given_match_id(self, json_data: Dict, match_id: str):
        logger.info(f"Getting delivery data for match id: {match_id}")
        teams = json_data["info"]["teams"]
        innings_data = json_data["innings"]
        for innings_number, innings in enumerate(innings_data, start=1):
            batting_team = innings.get('team')
            bowling_team = [team for team in teams if team != batting_team][0]
            self._get_delivery_data_of_single_innings(batting_team, bowling_team, innings, innings_number, match_id)

    def _get_delivery_data_of_single_innings(self, batting_team: str, bowling_team: str, innings_data: Dict, innings_number: int, match_id: int):
        overs_data = innings_data.get('overs')
        for single_over_data in overs_data:
            self._get_delivery_data_of_given_over(batting_team, bowling_team, innings_number, match_id, single_over_data)

    def _get_delivery_data_of_given_over(self, batting_team: str, bowling_team: str, innings_number: int, match_id: int, over_data: Dict):
        over_number = over_data.get('over')
        deliveries_data = over_data.get('deliveries')
        for ball_no, ball_data in enumerate(deliveries_data, start=1):
            single_delivery_data = self._get_delivery_data_of_single_delivery(
                ball_data, ball_no, batting_team, bowling_team, innings_number, match_id, over_number
            )
            self._deliveries_dataframe.loc[len(self._deliveries_dataframe)] = single_delivery_data

    def _get_delivery_data_of_single_delivery(
        self, ball_data: Dict, ball_number: int, batting_team: str, bowling_team: str, innings_number: int, match_id: int, over_number: int
    ):
        batter = ball_data.get("batter")
        bowler = ball_data.get("bowler")
        non_striker = ball_data.get("non_striker")

        extras_data = ball_data.get("extras", {})
        wide_runs = extras_data.get("wides", 0)
        leg_bye_runs = extras_data.get("legbyes", 0)
        bye_runs = extras_data.get("byes", 0)
        no_ball_runs = extras_data.get("noballs", 0)
        penalty_runs = extras_data.get("penalty", 0)

        batsman_runs = ball_data.get("runs", {}).get("batter", 0)
        extra_runs = ball_data.get("runs", {}).get("extras", 0)
        total_runs = ball_data.get("runs", {}).get("total", 0)

        wickets_data = ball_data.get("wickets", [])
        player_dismissed = None
        dismissal_type = None
        fielder_name = None
        if wickets_data:
            player_dismissed = wickets_data[0].get("player_out")
            dismissal_type = wickets_data[0].get("kind")
            fielder_name = wickets_data[0].get("fielders", [{}])[0].get("name")

        return {
            "match_id": match_id,
            "innings_number": innings_number,
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            "over_number": over_number,
            "ball_number": ball_number,
            "batter": batter,
            "bowler": bowler,
            "non_striker": non_striker,
            "wide_runs": wide_runs,
            "leg_bye_runs": leg_bye_runs,
            "bye_runs": bye_runs,
            "no_ball_runs": no_ball_runs,
            "penalty_runs": penalty_runs,
            "batsman_runs": batsman_runs,
            "extra_runs": extra_runs,
            "total_runs": total_runs,
            "player_dismissed": player_dismissed,
            "dismissal_type": dismissal_type,
            "fielder_name": fielder_name
        }

    def _load_existing_deliveries_dataframe(self) -> pd.DataFrame:
        try:
            s3_object = self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=self._deliveries_csv_file_key)
            csv_data = s3_object["Body"].read().decode("utf-8")
            return pd.read_csv(StringIO(csv_data))
        except self._s3_client.exceptions.NoSuchKey:
            return self._deliveries_dataframe

    def _load_json_data(self, json_file: str) -> dict:
        with open(json_file, "r") as file:
            data = json.load(file)
        return data

    def _list_all_the_json_files_in_the_extraction_folder(self) -> list:
        logger.info(f"Files in the extraction folder: {os.listdir(self._temporary_extraction_folder)}")
        try:
            json_files = []
            for _, _, files in os.walk(self._temporary_extraction_folder):
                for file in files:
                    if file.endswith(".json"):
                        json_files.append(os.path.join(self._temporary_extraction_folder, file))
            return json_files
        except Exception as e:
            logger.error(f"Error listing JSON files: {e}", exc_info=True)
            raise e

    def _process_single_json_file(self, json_file: str) -> None:
        json_data = self._load_json_data(json_file)
        match_id = os.path.splitext(os.path.basename(json_file))[0]
        self._get_delivery_data_of_given_match_id(json_data, match_id)

    def _update_deliveries_data_csv_file_with_newly_collected_data(self) -> None:
        updated_deliveries_dataframe = pd.concat([self._existing_deliveries_dataframe, self._deliveries_dataframe], ignore_index=True)
        csv_buffer = StringIO()
        updated_deliveries_dataframe.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        self._s3_client.put_object(Body=csv_data, Bucket=self._s3_bucket_name, Key=self._deliveries_csv_file_key)

    def _upload_processed_json_files_to_processed_folder_in_s3(self, processed_json_files):
        for json_file in processed_json_files:
            self._s3_client.upload_file(json_file, self._s3_bucket_name, f"{self._s3_folder_to_store_processed_json_files_zip}/{os.path.basename(json_file)}")

def handler(event, __):
    try:
        logger.info(f"Received event: {event}")
        handler = DeliverywiseCricsheetDataExtractionHandler()
        handler.extract_deliverywise_cricsheet_data(event=event)

        return {
            "statusCode": 200,
            "body": "Data Processed successfully"
        }
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": f"Internal Server Error: {str(e)}"
        }
