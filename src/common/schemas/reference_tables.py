from pyspark.sql.types import StringType, TimestampType, IntegerType, BooleanType, StructType, StructField

from src.common.schemas.table_definition import TableDefinition

DATASET_NAME = 'data_quality_ds'


class RulesTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_rule'
        )
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.RULE_TEMPLATE_ID_COL_NAME = 'rule_template_id'
        self.RULE_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.RULE_NAME_COL_NAME = 'rule_name'
        self.RULE_NAME_DATA_TYPE = StringType()
        self.RULE_DESCRIPTION_COL_NAME = 'rule_description'
        self.RULE_DESCRIPTION_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_ID_COL_NAME, self.RULE_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.RULE_NAME_COL_NAME, self.RULE_NAME_DATA_TYPE, True),
            StructField(self.RULE_DESCRIPTION_COL_NAME, self.RULE_DESCRIPTION_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleEntityMapTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_rule_entity_map'
        )
        self.RULE_ENTITY_ID_COL_NAME = 'rule_entity_id'
        self.RULE_ENTITY_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.ENTITY_ID_COL_NAME = 'entity_id'
        self.ENTITY_ID_DATA_TYPE = IntegerType()
        self.ENTITY_BEHAVIOUR_COL_NAME = 'entity_behaviour'
        self.ENTITY_BEHAVIOUR_DATA_TYPE = StringType()
        self.IS_PRIMARY_COL_NAME = 'is_primary'
        self.IS_PRIMARY_DATA_TYPE = BooleanType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_ENTITY_ID_COL_NAME, self.RULE_ENTITY_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.ENTITY_ID_COL_NAME, self.ENTITY_ID_DATA_TYPE, True),
            StructField(self.ENTITY_BEHAVIOUR_COL_NAME, self.ENTITY_BEHAVIOUR_DATA_TYPE, True),
            StructField(self.IS_PRIMARY_COL_NAME, self.IS_PRIMARY_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RulePropertiesTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_rule_properties'
        )
        self.RULE_PROP_ID_COL_NAME = 'rule_prop_id'
        self.RULE_PROP_ID_DATA_TYPE = IntegerType()
        self.RULE_ID_COL_NAME = 'rule_id'
        self.RULE_ID_DATA_TYPE = IntegerType()
        self.PROP_KEY_COL_NAME = 'prop_key'
        self.PROP_KEY_DATA_TYPE = StringType()
        self.PROP_VALUE_COL_NAME = 'prop_value'
        self.PROP_VALUE_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_PROP_ID_COL_NAME, self.RULE_PROP_ID_DATA_TYPE, True),
            StructField(self.RULE_ID_COL_NAME, self.RULE_ID_DATA_TYPE, True),
            StructField(self.PROP_KEY_COL_NAME, self.PROP_KEY_DATA_TYPE, True),
            StructField(self.PROP_VALUE_COL_NAME, self.PROP_VALUE_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleSetTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_rule_set'
        )
        self.RULESET_NAME_COL_NAME = 'ruleset_name'
        self.RULESET_NAME_DATA_TYPE = StringType()
        self.RULESET_DESCRIPTION_COL_NAME = 'ruleset_description'
        self.RULESET_DESCRIPTION_DATA_TYPE = StringType()
        self.NOTIFICATION_PREFERENCES_COL_NAME = 'notification_preferences'
        self.NOTIFICATION_PREFERENCES_DATA_TYPE = StringType()
        self.RULESET_ID_COL_NAME = 'ruleset_id'
        self.RULESET_ID_DATA_TYPE = IntegerType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.RULESET_NAME_COL_NAME, self.RULESET_NAME_DATA_TYPE, True),
            StructField(self.RULESET_DESCRIPTION_COL_NAME, self.RULESET_DESCRIPTION_DATA_TYPE, True),
            StructField(self.NOTIFICATION_PREFERENCES_COL_NAME, self.NOTIFICATION_PREFERENCES_DATA_TYPE, True),
            StructField(self.RULESET_ID_COL_NAME, self.RULESET_ID_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class RuleTemplateTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_rule_template'
        )
        self.RULE_TEMPLATE_ID_COL_NAME = 'rule_template_id'
        self.RULE_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.DQ_METRIC_COL_NAME = 'dq_metric'
        self.DQ_METRIC_DATA_TYPE = StringType()
        self.RULE_TEMPLATE_NAME_COL_NAME = 'rule_template_name'
        self.RULE_TEMPLATE_NAME_DATA_TYPE = StringType()
        self.RULE_TEMPLATE_DESC_COL_NAME = 'rule_template_desc'
        self.RULE_TEMPLATE_DESC_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = StringType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = StringType()

    def get_schema(self):
        return StructType([
            StructField(self.RULE_TEMPLATE_ID_COL_NAME, self.RULE_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.DQ_METRIC_COL_NAME, self.DQ_METRIC_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_NAME_COL_NAME, self.RULE_TEMPLATE_NAME_DATA_TYPE, True),
            StructField(self.RULE_TEMPLATE_DESC_COL_NAME, self.RULE_TEMPLATE_DESC_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])


class EntityTable(TableDefinition):

    def __init__(self):
        super().__init__(
            dataset_name=DATASET_NAME,
            table_name='dq_metadata_schema_entity'
        )
        self.ENTITY_ID_COL_NAME = 'entity_id'
        self.ENTITY_ID_DATA_TYPE = IntegerType()
        self.ENTITY_TEMPLATE_ID_COL_NAME = 'entity_template_id'
        self.ENTITY_TEMPLATE_ID_DATA_TYPE = IntegerType()
        self.ENTITY_NAME_COL_NAME = 'entity_name'
        self.ENTITY_NAME_DATA_TYPE = StringType()
        self.ENTITY_PHYSICAL_NAME_COL_NAME = 'entity_physical_name'
        self.ENTITY_PHYSICAL_NAME_DATA_TYPE = StringType()
        self.PRIMARY_KEY_COL_NAME = 'primary_key'
        self.PRIMARY_KEY_DATA_TYPE = StringType()
        self.CREATED_BY_COL_NAME = 'created_by'
        self.CREATED_BY_DATA_TYPE = StringType()
        self.CREATED_DATE_COL_NAME = 'created_date'
        self.CREATED_DATE_DATA_TYPE = TimestampType()
        self.UPDATED_BY_COL_NAME = 'updated_by'
        self.UPDATED_BY_DATA_TYPE = StringType()
        self.UPDATED_DATE_COL_NAME = 'updated_date'
        self.UPDATED_DATE_DATA_TYPE = TimestampType()

    def get_schema(self):
        return StructType([
            StructField(self.ENTITY_ID_COL_NAME, self.ENTITY_ID_DATA_TYPE, True),
            StructField(self.ENTITY_TEMPLATE_ID_COL_NAME, self.ENTITY_TEMPLATE_ID_DATA_TYPE, True),
            StructField(self.ENTITY_NAME_COL_NAME, self.ENTITY_NAME_DATA_TYPE, True),
            StructField(self.ENTITY_PHYSICAL_NAME_COL_NAME, self.ENTITY_PHYSICAL_NAME_DATA_TYPE, True),
            StructField(self.PRIMARY_KEY_COL_NAME, self.PRIMARY_KEY_DATA_TYPE, True),
            StructField(self.CREATED_BY_COL_NAME, self.CREATED_BY_DATA_TYPE, True),
            StructField(self.CREATED_DATE_COL_NAME, self.CREATED_DATE_DATA_TYPE, True),
            StructField(self.UPDATED_BY_COL_NAME, self.UPDATED_BY_DATA_TYPE, True),
            StructField(self.UPDATED_DATE_COL_NAME, self.UPDATED_DATE_DATA_TYPE, True)
        ])