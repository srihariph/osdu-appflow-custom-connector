
import json
import base64
import requests as api_requests
import boto3
import custom_connector_sdk.connector.auth as auth
import custom_connector_sdk.connector.configuration as config
import custom_connector_sdk.connector.settings as settings
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from utils.constants import (
    CONNECTOR_OWNER,
    CONNECTOR_NAME,
    CONNECTOR_VERSION,
    OSDU_ENTITLEMENT_API_ENDPOINT,
    PARAM_OSDU_URL,
    PARAM_DATA_PARTITION_ID,
    PARAM_AUTH_TOKEN_URL,
    PARAM_CLIENT_SECRET,
    PARAM_CLIENT_ID,
    PARAM_OAUTH_CUSTOM_SCOPE,
    AUTHENTICATION_TYPE,
    OAUTH_GRANT_TYPE,
    CONTENT_TYPE_ENCODED,
    CONNECTOR_RUNTIME_QUERY_KEY,
    OSDU_VERSION
)
from custom_connector_sdk.lambda_handler.handlers import (
    ConfigurationHandler
)


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


class ConnectorDemoConfigurationHandler(ConfigurationHandler):
    def validate_connector_runtime_settings(
        self, request: requests.ValidateConnectorRuntimeSettingsRequest
    ) -> responses.ValidateConnectorRuntimeSettingsResponse:
        print("ConfigurationHandler::validate_connector_runtime_settings")
        print(request.connector_runtime_settings)
        return responses.ValidateConnectorRuntimeSettingsResponse(is_success=True)

    def validate_credentials(
        self, request: requests.ValidateCredentialsRequest
    ) -> responses.ValidateCredentialsResponse:
        print("ConfigurationHandler::validate_credentials")

        secrets = boto3.client("secretsmanager").get_secret_value(
            SecretId=request.credentials.secret_arn
        )
        secret_string = json.loads(secrets["SecretString"])

        access_token = get_service_principal_token(
            secret_string[PARAM_CLIENT_ID], secret_string[PARAM_CLIENT_SECRET], secret_string[PARAM_AUTH_TOKEN_URL], secret_string[PARAM_OAUTH_CUSTOM_SCOPE])
        osdu_entitlements_url = secret_string[PARAM_OSDU_URL] + \
            OSDU_ENTITLEMENT_API_ENDPOINT
        payload = {}
        headers = {
            'data-partition-id': secret_string[PARAM_DATA_PARTITION_ID],
            'Authorization': 'Bearer ' + access_token
        }

        response = api_requests.request(
            "GET", osdu_entitlements_url, headers=headers, data=payload)
        if response.status_code == 200:
            print(
                f'SUCCESS: OSDU API was called successfully.. {response.status_code}')
        return responses.ValidateCredentialsResponse(is_success=True)

    def describe_connector_configuration(
        self, request: requests.DescribeConnectorConfigurationRequest
    ) -> responses.DescribeConnectorConfigurationResponse:
        print("ConfigurationHandler::describe_connector_configuration")

        osdu_authentication_config = auth.AuthenticationConfig(
            is_custom_auth_supported=True,
            custom_auth_config=[
                auth.CustomAuthConfig(
                    authentication_type=AUTHENTICATION_TYPE,
                    auth_parameters=[
                        auth.AuthParameter(
                            key=PARAM_OSDU_URL,
                            required=True,
                            label='Osdu On AWS Base URL',
                            description='DNS URL for osduonaws instance',
                            sensitive_field=False,
                            connector_supplied_values=None,
                        ),
                        auth.AuthParameter(
                            key=PARAM_DATA_PARTITION_ID,
                            required=True,
                            label='AWS OSDU Data Partition',
                            description='AWS OSDU Data Partition',
                            sensitive_field=False,
                            connector_supplied_values=None,
                        ),
                        auth.AuthParameter(
                            key=PARAM_AUTH_TOKEN_URL,
                            required=True,
                            label='AWS OSDU IDP Auth Token URL',
                            description='Auth Token URL from IDP',
                            sensitive_field=False,
                            connector_supplied_values=None,
                        ),
                        auth.AuthParameter(
                            key=PARAM_CLIENT_ID,
                            required=True,
                            label='Client Credentials Client ID ',
                            description='Client Credentials Client ID from IDP',
                            sensitive_field=False,
                            connector_supplied_values=None,
                        ),
                        auth.AuthParameter(
                            key=PARAM_CLIENT_SECRET,
                            required=True,
                            label='Client Credentials Client Secret ',
                            description='Client Credentials Client Secret from IDP',
                            sensitive_field=True,
                            connector_supplied_values=None,
                        ),
                        auth.AuthParameter(
                            key=PARAM_OAUTH_CUSTOM_SCOPE,
                            required=True,
                            label='OSDU Custom Scope',
                            description='OSDU Custom Scope set for this application in your IDP',
                            sensitive_field=False,
                            connector_supplied_values=None,
                        ),
                    ],
                )
            ],
        )
        osdu_query = settings.ConnectorRuntimeSetting(
            key=CONNECTOR_RUNTIME_QUERY_KEY,
            data_type=settings.ConnectorRuntimeSettingDataType.String,
            required=True,
            label="OSDU Search Query by Kind",
            description="OSDU Search Query by Kind",
            scope=settings.ConnectorRuntimeSettingScope.CONNECTOR_PROFILE,
        )

        return responses.DescribeConnectorConfigurationResponse(
            is_success=True,
            connector_owner=CONNECTOR_OWNER,
            connector_name=CONNECTOR_NAME,
            authentication_config=osdu_authentication_config,
            connector_version=CONNECTOR_VERSION,
            supported_api_versions=[OSDU_VERSION],
            connector_modes=[config.ConnectorModes.SOURCE,
                             config.ConnectorModes.DESTINATION],
            connector_runtime_setting=[osdu_query],
        )
