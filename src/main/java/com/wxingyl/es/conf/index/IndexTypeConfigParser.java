package com.wxingyl.es.conf.index;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.DbTableFieldDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.DefaultValueParser;

import java.util.*;

import static com.wxingyl.es.conf.ConfigKeyName.*;


/**
 * Created by xing on 15/8/13.
 * Note: type config have many tables, every table can config schema, dbAddress
 */
public class IndexTypeConfigParser implements ConfigParse<TypeConfigInfo> {

    private ThreadLocal<DefaultValueParser<String>> stringDefaultValueParser = new ThreadLocal<DefaultValueParser<String>>() {
        @Override
        protected DefaultValueParser<String> initialValue() {
            return new DefaultValueParser<String>((defaultValue, keyMap) -> {
                defaultValue.put(INDEX_DEFAULT_SCHEMA, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DB_ADDRESS, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DELETE_FIELD, new String[2]);
                defaultValue.put(INDEX_DEFAULT_DELETE_VALID_VALUE, new String[2]);

                keyMap.put(INDEX_DEFAULT_SCHEMA, INDEX_TABLE_SCHEMA);
                keyMap.put(INDEX_DEFAULT_DB_ADDRESS, INDEX_TABLE_DB_ADDRESS);
                keyMap.put(INDEX_DEFAULT_DELETE_FIELD, INDEX_TABLE_DELETE_FIELD);
                keyMap.put(INDEX_DEFAULT_DELETE_VALID_VALUE, INDEX_TABLE_DELETE_VALID_VALUE);
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
            stringDefaultValueParser.get().addDefaultValue(allTypes, 0);
            allTypes.forEach((type, v) -> {
                Map<String, Object> conf = (Map<String, Object>) v;
                List<Map<String, Object>> tablesConf = CommonUtils.getList(conf, INDEX_TYPE_INCLUDE_TABLE);
                if (tablesConf == null) {
                    throw new IndexConfigException("index: " + index + ", type: " + type + " need " + INDEX_TYPE_INCLUDE_TABLE + " config");
                }
                DbTableDesc masterTable = CommonUtils.getDbTable(conf, INDEX_TYPE_MASTER_TABLE, null);
                if (tablesConf.size() > 1 && masterTable == null) {
                    throw new IndexConfigException("index: " + index + ", type: " + type + " need " + INDEX_TYPE_MASTER_TABLE + " config");
                }
                stringDefaultValueParser.get().addDefaultValue(conf, 1);
                TypeConfigInfo typeInfo = new TypeConfigInfo();
                typeInfo.setIndexType(index, type);
                typeInfo.setMasterTable(masterTable);
                parseTableInfo(typeInfo, tablesConf);
                typeList.add(typeInfo);
            });
        });
        return typeList;
    }

    private void parseTableInfo(TypeConfigInfo typeInfo, List<Map<String, Object>> tablesConf) {
        Map<DbTableDesc, DbTableConfigInfo> tableInfoMap = new HashMap<>();
        DbTableConfigInfo masterTableInfo = null;
        DbTableDesc masterTable = typeInfo.getMasterTable();
        Map<DbTableConfigInfo, String> masterFiledMap = new HashMap<>();
        for (Map<String, Object> conf : tablesConf) {
            DbTableConfigInfo info = new DbTableConfigInfo();
            Map<String, String> defaultVal = stringDefaultValueParser.get().getDefaultValue(conf);
            info.initValue(typeInfo, conf, defaultVal);
            if (masterTable == null) {
                masterTable = info.getTable();
            }
            if ((masterTable.getSchema() == null && masterTable.getTable().equals(info.getTableName()))
                    || info.getTable().equals(typeInfo.getMasterTable())) {
                if (masterTableInfo != null) {
                    throw new IndexConfigException(info + " config find other master table");
                }
                masterTableInfo = info;
                if (masterTable.getSchema() == null) {
                    typeInfo.setMasterTable(DbTableDesc.build(info.getSchema(), masterTable.getTable()));
                    masterTable = typeInfo.getMasterTable();
                }
            } else {
                masterFiledMap.put(info, CommonUtils.getStringVal(conf, INDEX_TABLE_MASTER_FIELD));
            }
            tableInfoMap.put(info.getTable(), info);
        }
        if (masterTableInfo == null) {
            throw new IndexConfigException(typeInfo + " config, can't find a master-table");
        }
        final String finalMasterSchemaName = masterTable.getSchema();
        final String finalMasterTableName = masterTable.getTable();
        final DbTableConfigInfo finalMasterTableInfo = masterTableInfo;
        masterFiledMap.forEach((k, v) -> {
            DbTableFieldDesc masterField;
            if (v == null) {
                masterField = DbTableFieldDesc.build(finalMasterSchemaName, finalMasterTableName,
                        finalMasterTableInfo.getRelationField());
            } else {
                masterField = CommonUtils.getDbTableField(v, finalMasterSchemaName, finalMasterTableName);
                DbTableConfigInfo info = tableInfoMap.get(masterField.newDbTableDesc());
                if (info == null) {
                    throw new IndexConfigException("In table: " + k + ", " + INDEX_TABLE_MASTER_FIELD
                            + " config, can not find table: " + k.getMasterField());
                }
                info.addFiled(masterField.getField());
            }
            k.setMasterField(masterField);
        });
        typeInfo.setTables(new HashSet<>(tableInfoMap.values()));
    }

}
