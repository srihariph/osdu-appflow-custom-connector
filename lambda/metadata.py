import custom_connector_sdk.connector.auth as auth
import custom_connector_sdk.connector.configuration as config
import custom_connector_sdk.connector.context as context
import custom_connector_sdk.connector.fields as fields
import custom_connector_sdk.connector.settings as settings
import custom_connector_sdk.lambda_handler.requests as requests
import custom_connector_sdk.lambda_handler.responses as responses
from utils.constants import (
    OSDU_RESPONSE_FIELD_NAME,
    OSDU_API_ENTITY_IDENTIFIER
)
from custom_connector_sdk.lambda_handler.handlers import (
    MetadataHandler,

)

osdu_api_entity = context.Entity(
    entity_identifier=OSDU_API_ENTITY_IDENTIFIER,
    label="OSDU API Response",
    has_nested_entities=False,
    description="OSDU API Response",
)


class ConnectorDemoMetadataHandler(MetadataHandler):
    def list_entities(
        self, request: requests.ListEntitiesRequest
    ) -> responses.ListEntitiesResponse:
        print("MetadataHandler::list_entities")
        entity_list = [osdu_api_entity]
        return responses.ListEntitiesResponse(is_success=True, entities=entity_list)

    def describe_entity(
        self, request: requests.DescribeEntityRequest
    ) -> responses.DescribeEntityResponse:
        print("MetadataHandler::describe_entity")

        write_operation_types = set()
        write_operation_types.add(responses.WriteOperationType.UPSERT)

        osdu_response = context.FieldDefinition(
            field_name=OSDU_RESPONSE_FIELD_NAME,
            data_type=fields.FieldDataType.String,
            data_type_label="string",
            label="OSDU API Response",
            description="OSDU Record",
            default_value="osdu response",
            is_primary_key=True,
            read_properties=fields.ReadOperationProperty(
                is_queryable=True,
                is_retrievable=True,
                is_nullable=True
            ),
            write_properties=fields.WriteOperationProperty(
                is_creatable=True,
                is_updatable=True,
                is_defaulted_on_create=True,
                is_nullable=True,
                supported_write_operations=list(write_operation_types)

            )
        )

        entity_definition = context.EntityDefinition(
            entity=osdu_api_entity,
            fields=[osdu_response],
        )

        return responses.DescribeEntityResponse(
            is_success=True, entity_definition=entity_definition
        )
