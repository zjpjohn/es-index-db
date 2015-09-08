package com.wxingyl.es.conf.index;

import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.index.IndexTypeDesc;
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
public class IndexConfigParser implements IndexConfigParse {

    private ThreadLocal<DefaultValueParser<Integer>> integerDefaultValueParser = new ThreadLocal<DefaultValueParser<Integer>>() {
        @Override
        protected DefaultValueParser<Integer> initialValue() {
            return new DefaultValueParser<>((defaultValue, keyMap) -> {
                defaultValue.put(INDEX_DEFAULT_PAGE_SIZE, new Integer[2]);

                keyMap.put(INDEX_DEFAULT_PAGE_SIZE, INDEX_TABLE_PAGE_SIZE);
            });
        }
    };

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

    private TypeConfigInfo parseType(IndexTypeDesc typeDesc, Map<String, Object> typeConf) {
        List<Map<String, Object>> tablesConf = CommonUtils.getList(typeConf, INDEX_TYPE_INCLUDE_TABLE);
        if (tablesConf == null) {
            throw new IndexConfigException(typeDesc + " need " + INDEX_TYPE_INCLUDE_TABLE + " config");
        }
        DbTableDesc masterTable = CommonUtils.getDbTable(typeConf, INDEX_TYPE_MASTER_TABLE);
        if (tablesConf.size() > 1 && masterTable == null) {
            throw new IndexConfigException(typeDesc + " need " + INDEX_TYPE_MASTER_TABLE + " config");
        }
        stringDefaultValueParser.get().addDefaultValue(typeConf, 1);
        integerDefaultValueParser.get().addDefaultValue(typeConf, 1);

        TypeConfigInfo typeInfo = new TypeConfigInfo(typeDesc);
        Map<DbTableDesc, DbTableConfigInfo> tableInfoMap = new HashMap<>();
        DbTableConfigInfo masterTableInfo = null;
        Map<DbTableConfigInfo, String> tableMasterFiledMap = new HashMap<>();
        for (Map<String, Object> conf : tablesConf) {
            DbTableConfigInfo info = new DbTableConfigInfo();
            info.initValue(typeInfo, conf, stringDefaultValueParser.get(), integerDefaultValueParser.get());
            DbTableDesc tableDesc = info.getTable();
            if (masterTable == null) {
                masterTable = tableDesc;
            }
            if ((masterTable.getSchema() == null && masterTable.getTable().equals(tableDesc.getTable()))
                    || tableDesc.equalsIgnoreUrlAddress(masterTable)) {
                if (masterTableInfo != null) {
                    throw new IndexConfigException(info + " config find other master table");
                }
                masterTableInfo = info;
                if (masterTable.getSchema() == null) {
                    masterTable = tableDesc;
                }
            } else {
                tableMasterFiledMap.put(info, CommonUtils.getStringVal(conf, INDEX_TABLE_MASTER_FIELD));
            }
            tableInfoMap.put(tableDesc, info);
        }
        if (masterTableInfo == null) {
            throw new IndexConfigException(typeInfo + " config, can't find a master-table");
        }
        typeInfo.setMasterTable(masterTable);
        for (Map.Entry<DbTableConfigInfo, String> e : tableMasterFiledMap.entrySet()) {
            DbTableFieldDesc masterField;
            if (e.getValue() == null) {
                masterField = new DbTableFieldDesc(masterTable,
                        masterTableInfo.getRelationField());
            } else {
                masterField = CommonUtils.getDbTableField(e.getValue(), masterTable);
                DbTableConfigInfo info = tableInfoMap.get(masterField.newDbTableDesc());
                if (info == null) {
                    throw new IndexConfigException("In table: " + e.getKey() + ", " + INDEX_TABLE_MASTER_FIELD
                            + " config, can not find table: " + e.getKey().getMasterField());
                }
                info.addFiled(masterField.getField());
            }
            e.getKey().setMasterField(masterField);
        }
        typeInfo.setTables(new HashSet<>(tableInfoMap.values()));
        return typeInfo;
    }

    @Override
    public Set<TypeConfigInfo> parse(String index, Map<String, Object> indexConfig) {
        stringDefaultValueParser.get().addDefaultValue(indexConfig, 0);
        integerDefaultValueParser.get().addDefaultValue(indexConfig, 0);
        List<Map<String, Object>> types = CommonUtils.getList(indexConfig, INDEX_INCLUDE_TYPE);
        if (types == null) {
            throw new IndexConfigException("index: " + index + " config there is no config value: " + INDEX_INCLUDE_TYPE);
        }
        Set<TypeConfigInfo> result = new HashSet<>();
        types.forEach(typeConfig -> {
            String type = CommonUtils.getStringVal(typeConfig, INDEX_TYPE_TYPE);
            if (type == null) {
                throw new IndexConfigException("index: " + index + " type config there is a type no have " + INDEX_TYPE_TYPE + " value");
            }
            IndexTypeDesc typeDesc = new IndexTypeDesc(index, type);
            TypeConfigInfo typeInfo = parseType(typeDesc, typeConfig);
            if (typeInfo != null) result.add(typeInfo);
        });
        return result;
    }
}
