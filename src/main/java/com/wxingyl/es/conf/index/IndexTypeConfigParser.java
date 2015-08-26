package com.wxingyl.es.conf.index;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.DefaultValueParser;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;

import static com.wxingyl.es.conf.ConfigKeyName.*;


/**
 * Created by xing on 15/8/13.
 * Note: type config have many tables, every table can config schema, dbAddress
 */
public class IndexTypeConfigParser implements ConfigParse<TypeConfigInfo> {

    private ThreadLocal<DefaultValueParser<String>> defaultValueParser = new ThreadLocal<DefaultValueParser<String>>() {
        @Override
        protected DefaultValueParser<String> initialValue() {
            return new DefaultValueParser<String>((defaultValue, keyMap) -> {
                defaultValue.put(INDEX_DEFAULT_SCHEMA, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DB_ADDRESS, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DELETE_FIELD, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DELETE_VALID_VALUE, new String[2]);

                keyMap.put(INDEX_DEFAULT_SCHEMA, INDEX_SCHEMA);
                keyMap.put(INDEX_DEFAULT_DB_ADDRESS, INDEX_DB_ADDRESS);
                keyMap.put(INDEX_DEFAULT_DELETE_FIELD, INDEX_DELETE_FIELD);
                keyMap.put(INDEX_DEFAULT_DELETE_VALID_VALUE, INDEX_DELETE_VALID_VALUE);
            }) {
                @Override
                protected String getVal(Map<String, Object> confMap, String key) {
                    return CommonUtils.getStringVal(confMap, key);
                }
            };
        }
    };

    @SuppressWarnings("unchecked")
    @Override
    public Set<TypeConfigInfo> parse(Map<String, Object> map) {
        Set<TypeConfigInfo> typeList = new HashSet<>();
        map.forEach((index, t) -> {
            Map<String, Object> allTypes = (Map<String, Object>) t;
            defaultValueParser.get().addDefaultValue(allTypes, 0);
            allTypes.forEach((type, v) -> {
                Map<String, Object> conf = (Map<String, Object>) v;
                String masterTable = CommonUtils.getStringVal(conf, INDEX_MASTER_TABLE);
                if (masterTable == null) {
                    throw new IndexConfigException("index: " + index + ", type: " + type + " need " + INDEX_MASTER_TABLE + " config");
                }
                List<Map<String, Object>> tablesConf = CommonUtils.getList(conf, INDEX_INCLUDE_TABLE);
                if (tablesConf == null) {
                    throw new IndexConfigException("index: " + index + ", type: " + type + " need " + INDEX_INCLUDE_TABLE + " config");
                }
                defaultValueParser.get().addDefaultValue(conf, 1);
                TypeConfigInfo typeParse = new TypeConfigInfo();
                typeParse.index = index;
                typeParse.type = type;
                typeParse.masterTable = masterTable;
                parseTableInfo(typeParse, tablesConf);
                typeList.add(typeParse);
            });
        });
        return typeList;
    }

    private void parseTableInfo(TypeConfigInfo typeParse, List<Map<String, Object>> tablesConf) {
        Set<DbTableConfigInfo> tableInfoParseSet = new HashSet<>();
        DbTableConfigInfo masterTable = null;
        final Map<String, List<String>> tableDependFields = new HashMap<>();
        typeParse.dependedTable = new HashMap<>();
        for (Map<String, Object> conf : tablesConf) {
            DbTableConfigInfo info = new DbTableConfigInfo();
            defaultValueParser.get().getDefaultValue(conf).forEach(info::setDefaultValue);
            String tableName = CommonUtils.getStringVal(conf, INDEX_TABLE_NAME);
            if (tableName == null) {
                throw new IndexConfigException("table_name conf is null of " + typeParse);
            }
            info.tableName = tableName;
            if (info.schema == null) {
                throw new IndexConfigException(typeParse + " conf, table_name: " + tableName + " can't find schema");
            }
            if (info.deleteField != null && info.deleteValidValue == null) {
                throw new IndexConfigException(typeParse + " conf, table_name: " + tableName + " delete_field: "
                        + info.deleteField + ", but delete_valid_value is null");
            }
            String relationField = CommonUtils.getStringVal(conf, INDEX_RELATION_FIELD);
            if (relationField == null) {
                throw new IndexConfigException(typeParse + ", table_name: " + tableName
                        + " need " + INDEX_RELATION_FIELD + " config");
            }
            info.relationField = relationField;
            info.forbidFields = CommonUtils.getSet(conf, INDEX_FORBID_FIELDS);
            info.fields = CommonUtils.getSet(conf, INDEX_FIELDS);
            boolean isMasterTable = false;
            if (tableName.equals(typeParse.masterTable)) {
                masterTable = info;
                isMasterTable = true;
            }
            String masterField = CommonUtils.getStringVal(conf, INDEX_MASTER_FIELD);
            String dependTable = typeParse.masterTable;
            if (!isMasterTable && masterField != null) {
                if (masterField.indexOf(',') > 0) {
                    String[] arr = masterField.split(",");
                    info.masterField = Tuple.tuple(arr[0], arr[1]);
                    dependTable = info.masterField.v1();
                } else {
                    info.masterField = Tuple.tuple(typeParse.masterTable, masterField);
                }
                List<String> list = tableDependFields.get(info.masterField.v1());
                if (list == null) {
                    tableDependFields.put(info.masterField.v1(), list = new ArrayList<>());
                }
                list.add(info.masterField.v2());
            }
            if (!isMasterTable) {
                Set<String> set = typeParse.dependedTable.get(dependTable);
                if (set == null) {
                    typeParse.dependedTable.put(dependTable, set = new HashSet<>());
                }
                set.add(tableName);
            }
            tableInfoParseSet.add(info);
        }
        if (masterTable == null) {
            throw new IndexConfigException(typeParse + " config, can't find a master-table");
        }
        tableInfoParseSet.forEach(info -> {
            List<String> dependFields = tableDependFields.get(info.tableName);
            if (CommonUtils.isEmpty(info.fields) || info.fields.contains("*")) {
                info.fields = null;
            } else if (!CommonUtils.isEmpty(info.forbidFields)) {
                info.fields.removeAll(info.forbidFields);
                if (info.fields.isEmpty()) {
                    throw new IndexConfigException(typeParse + " fields retain forbid_fields is empty, forbid_fields: "
                            + info.forbidFields);
                }
                //fields is not empty, so forbidFields it useless
                info.forbidFields = null;
            }
            if (info.fields != null) {
                info.fields.add(info.relationField);
                if (dependFields != null) info.fields.addAll(dependFields);
            } else if (info.forbidFields != null) {
                info.forbidFields.remove(info.relationField);
                if (dependFields != null) info.forbidFields.removeAll(dependFields);
            }
        });
        typeParse.tables = tableInfoParseSet;
    }

}
