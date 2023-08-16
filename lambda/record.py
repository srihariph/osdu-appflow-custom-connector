import json
import logging
import base64
import requests as api_requests
import boto3
import custom_connector_sdk.connector.context as context
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from utils.constants import (
    OSDU_SEARCH_QUERY_KIND,
    OSDU_DATA_PARTITION,
    OSDU_SEARCH_ENDPOINT,
    OSDU_STORAGE_ENDPOINT,
    OSDU_SEARCH_ENDPOINT,
    OAUTH_GRANT_TYPE,
    CONTENT_TYPE_ENCODED,
    CONTENT_TYPE_JSON,
    OSDU_RESPONSE_FIELD_NAME,
    PARAM_OSDU_URL,
    PARAM_DATA_PARTITION_ID,
    PARAM_AUTH_TOKEN_URL,
    PARAM_CLIENT_SECRET,
    PARAM_CLIENT_ID,
    PARAM_OAUTH_CUSTOM_SCOPE,
    OSDU_API_ENTITY_IDENTIFIER
)
from custom_connector_sdk.lambda_handler.handlers import (
    RecordHandler,
)
from custom_connector_sdk.lambda_handler.responses import ErrorCode, ErrorDetails

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_service_principal_token(client_id: str, client_secret: str, auth_token_url: str, oauth_custom_scope: str) -> str:
    print("inside get_service_principal_token")

    auth = '{}:{}'.format(client_id, client_secret)
    encoded_auth = base64.b64encode(str.encode(auth))

    headers = {}
    headers['Authorization'] = 'Basic ' + encoded_auth.decode()
    headers['Content-Type'] = CONTENT_TYPE_ENCODED

    data = {
        "grant_type": OAUTH_GRANT_TYPE,
        "scope": oauth_custom_scope
    }
    response = api_requests.post(
        url=auth_token_url, headers=headers, data=data)

    return json.loads(response.content.decode())['access_token']


def osdu_search(secret_string: str, query_string: str):

    access_token = get_service_principal_token(
        secret_string[PARAM_CLIENT_ID], secret_string[PARAM_CLIENT_SECRET], secret_string[PARAM_AUTH_TOKEN_URL], secret_string[PARAM_OAUTH_CUSTOM_SCOPE])
    osdu_search_url = secret_string[PARAM_OSDU_URL] + \
        OSDU_SEARCH_ENDPOINT

    payload = json.dumps(
        {"kind": query_string})
    headers = {
        'Content-Type': CONTENT_TYPE_JSON,
        'data-partition-id': secret_string[PARAM_DATA_PARTITION_ID],
        'Authorization': 'Bearer ' + access_token
    }
    res = api_requests.request(
        "POST", osdu_search_url, headers=headers, data=payload)

    return res


def osdu_get_storage(secret_string: str, record_id: str):
    access_token = get_service_principal_token(
        secret_string[PARAM_CLIENT_ID], secret_string[PARAM_CLIENT_SECRET], secret_string[PARAM_AUTH_TOKEN_URL], secret_string[PARAM_OAUTH_CUSTOM_SCOPE])
    headers = {
        'data-partition-id': secret_string[PARAM_DATA_PARTITION_ID],
        'Authorization': 'Bearer ' + access_token
    }
    payload = {}
    osdu_storage_url = secret_string[PARAM_OSDU_URL] + \
        OSDU_STORAGE_ENDPOINT + "/" + record_id
    res = api_requests.request(
        "GET", osdu_storage_url, headers=headers, data=payload)
    return res.text


def osdu_put_storage(secret_string: str, storage_string: str):
    access_token = get_service_principal_token(
        secret_string[PARAM_CLIENT_ID], secret_string[PARAM_CLIENT_SECRET], secret_string[PARAM_AUTH_TOKEN_URL], secret_string[PARAM_OAUTH_CUSTOM_SCOPE])
    headers = {
        'Content-Type': CONTENT_TYPE_JSON,
        'data-partition-id': secret_string[PARAM_DATA_PARTITION_ID],
        'Authorization': 'Bearer ' + access_token
    }
    record_json = json.loads(storage_string)
    payload = json.dumps([record_json])
    osdu_storage_url = secret_string[PARAM_OSDU_URL] + \
        OSDU_STORAGE_ENDPOINT
    response = api_requests.request("PUT", osdu_storage_url, headers=headers, data=payload)
    print(payload)
    print(response)
    return response.text


osdu_api_entity = context.Entity(
    entity_identifier=OSDU_API_ENTITY_IDENTIFIER,
    label="OSDU API Response",
    has_nested_entities=False,
    description="OSDU API Response",
)


class ConnectorDemoRecordHandler(RecordHandler):
    def retrieve_data(
        self, request: requests.RetrieveDataRequest
    ) -> responses.RetrieveDataResponse:
        print("RecordHandler::retrieve_data")

        record_list = []
        return responses.RetrieveDataResponse(is_success=True, records=record_list)

    def write_data(
        self, request: requests.WriteDataRequest
    ) -> responses.WriteDataResponse:
        print("RecordHandler::write_data")
        write_record_results = []
        secrets = boto3.client("secretsmanager").get_secret_value(
            SecretId=request.connector_context.credentials.secret_arn
        )
        secret_string = json.loads(secrets["SecretString"])        
        for record in request.records:
            record_json = json.loads(record)
            print(record_json[OSDU_RESPONSE_FIELD_NAME])
            res=osdu_put_storage(secret_string,record_json[OSDU_RESPONSE_FIELD_NAME])
            print(res)
        return responses.WriteDataResponse(
            is_success=True, write_record_results=write_record_results
        )

    def query_data(
        self, request: requests.QueryDataRequest
    ) -> responses.QueryDataResponse:
        print("RecordHandler::query_data")

        if OSDU_SEARCH_QUERY_KIND not in request.connector_context.connector_runtime_settings:
            error_message = f"{OSDU_SEARCH_QUERY_KIND} should be provided as runtime setting"
            LOGGER.error(
                f"QueryData request failed with entity: {error_message}")
            return responses.QueryDataResponse(
                is_success=False,
                error_details=ErrorDetails(
                    error_code=ErrorCode.InvalidArgument,
                    error_message=error_message,
                ),
            )

        entity_id = request.connector_context.entity_definition.entity.entity_identifier
        if entity_id != osdu_api_entity.entity_identifier:
            error_message = f"{entity_id} is not valid entity"
            LOGGER.error(
                f"QueryData request failed with entity: {error_message}")
            return responses.QueryDataResponse(
                is_success=False,
                error_details=ErrorDetails(
                    error_code=ErrorCode.InvalidArgument,
                    error_message=error_message,
                ),
            )
        secrets = boto3.client("secretsmanager").get_secret_value(
            SecretId=request.connector_context.credentials.secret_arn
        )
        secret_string = json.loads(secrets["SecretString"])
        search_results = osdu_search(
            secret_string, request.connector_context.connector_runtime_settings[OSDU_SEARCH_QUERY_KIND])
        search_results_json = search_results.json()
        storage_results = []
        record_list = []
        for record in search_results_json['results']:
            storage_record = osdu_get_storage(secret_string, record['id'])
            storage_results.append(storage_record)
        print(storage_results)
        for storage_record in storage_results:
            osdu_record = json.dumps(
                {
                    "osdurecord": storage_record,
                }
            )
            record_list.append(osdu_record)
            print(record_list)
        return responses.QueryDataResponse(is_success=True, records=record_list)
