
from configuration import ConnectorDemoConfigurationHandler
from metadata import ConnectorDemoMetadataHandler
from record import ConnectorDemoRecordHandler
from custom_connector_sdk.lambda_handler.lambda_handler import (
    BaseLambdaConnectorHandler,
)


class ConnectorDemoLambdaHandler(BaseLambdaConnectorHandler):
    def __init__(self):
        super().__init__(
            ConnectorDemoMetadataHandler(),
            ConnectorDemoRecordHandler(),
            ConnectorDemoConfigurationHandler(),
        )


def handler(event, context):
    return ConnectorDemoLambdaHandler().lambda_handler(event, context)