import json
import logging
from typing import Dict
import boto3
from pymongo import MongoClient
from mens_t20i_data_collector._lambdas.utils import (
    exception_handler,
    get_environmental_variable_value,
    make_dynamodb_entry_for_file_data_extraction_status,
    parse_sns_event_message
)

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MatchwiseCricsheetDataExtractionHandler:

    def __init__(self, match_id: int):
        """
        Initializes the handler with the match ID and sets up required resources like S3 and DynamoDB.
        :param match_id: Match ID for which delivery data needs to be extracted
        """
        self._match_id = match_id
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._mongo_db_url = get_environmental_variable_value("MONGO_DB_URL")
        self._mongo_db_name = get_environmental_variable_value("MONGO_DB_NAME")
        self._mongo_collection_name = get_environmental_variable_value("MATCHWISE_DATA_COLLECTION_NAME")
        self._mongo_db_client = MongoClient(self._mongo_db_url)
        self._matchwise_data_mongo_collection = self._mongo_db_client[self._mongo_db_name][self._mongo_collection_name]
        self._s3_client = boto3.client("s3")
        dynamodb_client = boto3.resource("dynamodb")
        self._dynamo_db_to_store_file_data_extraction_status = dynamodb_client.Table(   # type: ignore
            get_environmental_variable_value("DYNAMODB_TABLE_NAME")
        )

    def extract_matchwise_cricsheet_data(self, json_s3_file_key: str) -> None:
        """
        Extracts matchwise data from the S3 JSON file, processes it, and stores the data in DynamoDB.
        :param json_s3_file_key: The S3 file key for the cricsheet JSON file
        """
        logger.info(f"Extracting matchwise cricsheet data from {json_s3_file_key}")
        try:
            bytes_buffer = self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=json_s3_file_key)["Body"].read()
            json_data = json.loads(bytes_buffer)
            self._get_match_data_of_given_match_id_and_store_in_dynamodb(json_data)
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}", exc_info=True)
            raise

    def _get_match_data_of_given_match_id_and_store_in_dynamodb(self, json_data: Dict) -> None:
        """
        Processes the JSON data and stores it in DynamoDB.
        :param json_data: The JSON data containing match information
        """
        info = json_data.get('info', {})
        teams = info.get('teams', [])
        match_data = {
            "index": int(info.get('match_type_number')),
            "match_id": self._match_id,
            "date": info.get('dates', [None])[0],
            "event_name": info.get('event', {}).get('name'),
            "ground_name": info.get('venue'),
            "ground_city": info.get('city'),
            "team_1": teams[0] if teams else None,
            "team_2": teams[1] if len(teams) > 1 else None,
            "toss_winner": info.get('toss', {}).get('winner'),
            "toss_decision": info.get('toss', {}).get('decision'),
            "winner": info.get('outcome', {}).get('winner') or info.get('outcome', {}).get('result'),
            "margin_runs": info.get('outcome', {}).get('by', {}).get('runs'),
            "margin_wickets": info.get('outcome', {}).get('by', {}).get('wickets'),
            "player_of_the_match": info.get('player_of_match', [None])[0]
        }
        self._store_dataframe_in_mongodb(match_data)
        make_dynamodb_entry_for_file_data_extraction_status(
            table=self._dynamo_db_to_store_file_data_extraction_status,
            file_name=f"{self._match_id}.json",
            field="matchwise_data_extraction_status",
            status=True
        )

    def _store_dataframe_in_mongodb(self, match_data: Dict) -> None:
        """
        Stores the match dataframe in MongoDB.
        """
        match_data['_id'] = match_data['index']
        logger.info(f"Inserting match data for match {match_data['index']} in MongoDB...")
        try:
            self._matchwise_data_mongo_collection.insert_one(match_data)
            logger.info("Data stored in MongoDB successfully")
        except Exception as e:
            logger.error(f"Failed to store data in MongoDB: {e}")
            raise


@exception_handler      # noqa: Vulture
@parse_sns_event_message
def handler(json_file_key, match_id):
    matchwise_cricsheet_data_extraction_handler = MatchwiseCricsheetDataExtractionHandler(match_id)
    matchwise_cricsheet_data_extraction_handler.extract_matchwise_cricsheet_data(json_file_key)
    return "Matchwise cricsheet data extraction completed successfully!"
