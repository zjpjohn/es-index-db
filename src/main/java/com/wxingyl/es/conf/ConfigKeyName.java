package com.wxingyl.es.conf;

/**
 * Created by xing on 15/8/19.
 * define key name of config file
 */
public interface ConfigKeyName {

    String DS_DATA_SOURCE = "data_source";

    String DS_DRIVER_CLASS_NAME = "driver_class_name";

    String DS_SCHEMA_LIST = "schemas";

    String DS_URL = "url";

    String DS_USERNAME = "username";

    String DS_DB_NAMES = "db_names";

    String DS_PASSWORD = "password";

    String INDEX_DEFAULT_SCHEMA = "default_schema";

    String INDEX_DEFAULT_DB_ADDRESS = "default_db_address";

    String INDEX_DEFAULT_DELETE_FIELD = "default_delete_field";

    String INDEX_DEFAULT_DELETE_VALID_VALUE = "default_delete_valid_value";

    String INDEX_INCLUDE_TABLE = "include_table";

    String INDEX_SCHEMA = "schema";

    String INDEX_DB_ADDRESS = "db_address";

    String INDEX_DELETE_FIELD = "delete_field";

    String INDEX_DELETE_VALID_VALUE = "delete_valid_value";

    String INDEX_MASTER_TABLE = "master_table";

    String INDEX_TABLE_NAME = "table_name";

    String INDEX_FIELDS = "fields";

    String INDEX_FORBID_FIELDS = "forbid_fields";

    String INDEX_MASTER_FIELD = "master_field";

    String INDEX_RELATION_FIELD = "relation_field";

}
