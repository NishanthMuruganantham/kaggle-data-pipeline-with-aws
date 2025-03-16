import json
import logging
from typing import Dict
import boto3
import pandas as pd
from pymongo import MongoClient
from mens_t20i_data_collector._lambdas.constants import (
    DELIVERYWISE_DATAFRAME_COLUMNS
)
from mens_t20i_data_collector._lambdas.utils import (
    exception_handler,
    get_environmental_variable_value,
    parse_sns_event_message
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DeliverywiseCricsheetDataExtractionHandler:

    def __init__(self, match_id: int):
        """
        Initializes the handler with the match ID and sets up required resources like S3 and DynamoDB.

        :param match_id: Match ID for which delivery data needs to be extracted
        """
        self._match_id = match_id
        self._s3_bucket_name = get_environmental_variable_value("DOWNLOAD_BUCKET_NAME")
        self._mongo_db_url = get_environmental_variable_value("MONGO_DB_URL")
        self._mongo_db_name = get_environmental_variable_value("MONGO_DB_NAME")
        self.collection_name = get_environmental_variable_value("DELIVERYWISE_DATA_COLLECTION_NAME")
        self._mongo_db_client = MongoClient(self._mongo_db_url)
        self._deliverywise_data_mongo_collection = self._mongo_db_client[self._mongo_db_name][self.collection_name]
        self._s3_client = boto3.client("s3")
        self._deliveries_dataframe: pd.DataFrame = pd.DataFrame(columns=DELIVERYWISE_DATAFRAME_COLUMNS) # type: ignore

    def extract_deliverywise_cricsheet_data(self, json_s3_file_key: str) -> None:
        """
        Extracts delivery data from the S3 JSON file, processes it, and stores the data in DynamoDB.

        :param json_s3_file_key: The S3 file key for the cricsheet JSON file
        """
        logger.info(f"Extracting deliverywise cricsheet data from {json_s3_file_key}")
        try:
            bytes_buffer = self._s3_client.get_object(Bucket=self._s3_bucket_name, Key=json_s3_file_key)["Body"].read()
            json_data = json.loads(bytes_buffer)
            self._get_delivery_data_of_given_match_id(json_data)
            self._correct_datatypes_and_create_composite_delivery_key_to_store_dataframe_in_dynamo_db()
            self._store_dataframe_in_mongodb()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in the file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error occurred: {e}", exc_info=True)
            raise

    def _correct_datatypes_and_create_composite_delivery_key_to_store_dataframe_in_dynamo_db(self) -> None:
        """
        Corrects the datatypes in the dataframe and creates a composite key for storing in DynamoDB.
        """

        logger.info("Correcting datatypes and creating composite delivery key...")
        primary_key_columns = ["match_id", "innings_number", "over_number", "ball_number"]
        self._deliveries_dataframe["composite_delivery_key"] = self._deliveries_dataframe[primary_key_columns].apply(tuple, axis=1)
        self._deliveries_dataframe["composite_delivery_key"] = self._deliveries_dataframe["composite_delivery_key"].astype(str)
        logger.info("Composite delivery key created successfully")

    def _store_dataframe_in_mongodb(self) -> None:
        """
        Stores the deliveries dataframe in MongoDB.
        """
        logger.info(f"Storing {len(self._deliveries_dataframe)} records in MongoDB...")
        try:
            df_with_id = self._deliveries_dataframe.copy()
            df_with_id['_id'] = df_with_id['composite_delivery_key']
            records = df_with_id.to_dict("records")
            self._deliverywise_data_mongo_collection.insert_many(records)
            logger.info("Data stored in MongoDB successfully")
        except Exception as e:
            logger.error(f"Failed to store data in MongoDB: {e}")
            raise

    def _get_delivery_data_of_given_match_id(self, json_data: Dict) -> None:

        logger.info(f"Extracting delivery data for match ID: {self._match_id}")
        try:
            teams = json_data["info"]["teams"]
            innings_data = json_data["innings"]
            for innings_number, innings in enumerate(innings_data, start=1):
                batting_team = innings.get('team')
                bowling_team = [team for team in teams if team != batting_team][0]
                self._get_delivery_data_of_single_innings(batting_team, bowling_team, innings, innings_number)

        except KeyError as e:
            logger.error(f"Missing expected key in JSON data: {e}")
            raise

    def _get_delivery_data_of_single_innings(self, batting_team: str, bowling_team: str, innings_data: Dict, innings_number: int) -> None:
        """
        Processes delivery data for a single innings and appends it to the dataframe.

        :param batting_team: The batting team for the innings
        :param bowling_team: The bowling team for the innings
        :param innings_data: The innings data from the cricsheet JSON
        :param innings_number: The innings number (1 or 2)
        """
        overs_data = innings_data.get('overs', [])
        for over_data in overs_data:
            self._get_delivery_data_of_given_over(batting_team, bowling_team, innings_number, over_data)

    def _get_delivery_data_of_given_over(self, batting_team: str, bowling_team: str, innings_number: int, over_data: Dict) -> None:
        """
        Processes delivery data for a given over and appends it to the dataframe.

        :param batting_team: The batting team
        :param bowling_team: The bowling team
        :param innings_number: The innings number
        :param over_data: The over data from the cricsheet JSON
        """
        over_number = over_data["over"]
        deliveries_data = over_data.get('deliveries', [])

        for ball_no, ball_data in enumerate(deliveries_data, start=1):
            delivery_record = self._get_delivery_data_of_single_delivery(ball_data, ball_no, batting_team, bowling_team, innings_number, over_number)
            self._deliveries_dataframe = pd.concat([self._deliveries_dataframe, pd.DataFrame([delivery_record])], ignore_index=True)

    def _get_delivery_data_of_single_delivery(  # pylint: disable=[too-many-arguments, too-many-locals]
        self, ball_data: Dict, ball_number: int, batting_team: str, bowling_team: str, innings_number: int, over_number: int
    ) -> Dict:
        """
        Extracts and returns delivery data for a single ball.

        :param ball_data: Ball data from the cricsheet JSON
        :param ball_number: The ball number in the over
        :param batting_team: The batting team
        :param bowling_team: The bowling team
        :param innings_number: The innings number
        :param over_number: The over number
        :return: A dictionary representing a single delivery
        """
        batter = ball_data.get("batter")
        bowler = ball_data.get("bowler")
        non_striker = ball_data.get("non_striker")

        # Extras
        extras_data = ball_data.get("extras", {})
        wide_runs = extras_data.get("wides", 0)
        leg_bye_runs = extras_data.get("legbyes", 0)
        bye_runs = extras_data.get("byes", 0)
        no_ball_runs = extras_data.get("noballs", 0)
        penalty_runs = extras_data.get("penalty", 0)

        # Runs
        batsman_runs = ball_data.get("runs", {}).get("batter", 0)
        extra_runs = ball_data.get("runs", {}).get("extras", 0)
        total_runs = ball_data.get("runs", {}).get("total", 0)

        # Wickets
        wickets_data = ball_data.get("wickets", [])
        player_dismissed = None
        dismissal_type = None
        fielder_name = None
        if wickets_data:
            player_dismissed = wickets_data[0].get("player_out")
            dismissal_type = wickets_data[0].get("kind")
            fielder_name = wickets_data[0].get("fielders", [{}])[0].get("name")

        return {
            "match_id": self._match_id,
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


@exception_handler      # noqa: Vulture
@parse_sns_event_message
def handler(json_file_key, match_id):
    extractor = DeliverywiseCricsheetDataExtractionHandler(match_id)
    extractor.extract_deliverywise_cricsheet_data(json_file_key)
    return {
        "statusCode": 200,
        "body": "Data Processed successfully"
    }
