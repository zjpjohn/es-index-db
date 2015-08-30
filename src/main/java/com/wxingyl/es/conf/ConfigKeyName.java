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

    String DS_PASSWORD = "password";

    String DS_DB_NAMES = "db_names";

    String INDEX_DEFAULT_SCHEMA = "default_schema";

    String INDEX_DEFAULT_DB_ADDRESS = "default_db_address";

    String INDEX_DEFAULT_DELETE_FIELD = "default_delete_field";

    String INDEX_DEFAULT_DELETE_VALID_VALUE = "default_delete_valid_value";

    String INDEX_TYPE_MASTER_TABLE = "master_table";

    String INDEX_TYPE_INCLUDE_TABLE = "include_table";

    String INDEX_TABLE_SCHEMA = "schema";

    String INDEX_TABLE_DB_ADDRESS = "db_address";

    String INDEX_TABLE_DELETE_FIELD = "delete_field";

    String INDEX_TABLE_DELETE_VALID_VALUE = "delete_valid_value";

    String INDEX_TABLE_TABLE = "table";

    String INDEX_TABLE_FIELDS = "fields";

    String INDEX_TABLE_FORBID_FIELDS = "forbid_fields";

    String INDEX_TABLE_MASTER_FIELD = "master_field";

    String INDEX_TABLE_RELATION_FIELD = "relation_field";

    String INDEX_TABLE_PAGE_SIZE = "page_size";

}
