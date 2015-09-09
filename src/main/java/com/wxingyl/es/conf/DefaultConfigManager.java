package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.*;
import com.wxingyl.es.conf.index.*;
import com.wxingyl.es.dbquery.DefaultResultSetHandlerFactory;
import com.wxingyl.es.dbquery.ResultSetHandlerFactory;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.dbquery.DbTableDesc;
import com.wxingyl.es.dbquery.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Created by xing on 15/8/24.
 * default data source manager
 */
public class DefaultConfigManager extends AbstractConfigManager {

    private IndexConfigParse indexConfParser;

    private DataSourceParserManager dataSourceConfigFactory;

    private ResultSetHandlerFactory resultSetHandlerFactory;

    private BiConsumer<TableQueryInfo, List<String>> masterAliasVerify;

    /**
     * default add mysql parser
     */
    public DefaultConfigManager() {
        indexConfParser = new IndexConfigParser();
        dataSourceConfigFactory = new DataSourceParseFactory();
        dataSourceConfigFactory.addDataSourceConfigParser(new MysqlDataSourceConfigParser());
        resultSetHandlerFactory = DefaultResultSetHandlerFactory.INSTANCE;
        masterAliasVerify = this::verifyMasterAliasRepeat;
    }

    @Override
    protected Set<IndexTypeBean> transformToIndexTypeBean(Set<TypeConfigInfo> typeSet) {
        if (CommonUtils.isEmpty(typeSet)) return null;
        IndexTypeBean.Builder builder = IndexTypeBean.build();
        Set<IndexTypeBean> set = new HashSet<>();
        for (TypeConfigInfo type : typeSet) {
            builder.type(type.getTypeDesc());
            for (DbTableConfigInfo tableInfo : type.getTables()) {
                builder.addTableQuery(verifyTypeTableConfig(type, tableInfo), tableInfo,
                        resultSetHandlerFactory.get(this, tableInfo));
            }
            set.add(builder.build(type.getMasterTable(), masterAliasVerify));
        }
        return set;
    }

    private SqlQueryHandle verifyTypeTableConfig(TypeConfigInfo type, DbTableConfigInfo info) {
        DbTableDesc table = info.getTable();
        DataSourceBean dataSourceBean = findDataSourceBean(table.getSchema(), table.getUrlAddress());
        if (dataSourceBean == null) {
            throw new IndexConfigException("Index config: " + type + ", " + table + " can't find datasource config");
        }
        SqlQueryHandle handle = dataSourceBean.getQueryHandle();
        Set<String> allFields;
        try {
            //to verify every table is really exist
            handle.getAllTables(table.getSchema());
            allFields = handle.getAllFields(info.getTable());
        } catch (ExecutionException e) {
            throw new IndexConfigException("get " + info + " tables and fields have crash: " + e.getMessage(), e);
        }
        if (info.getFields() != null) {
            for (String s : info.getFields()) {
                if (!allFields.contains(s)) {
                    throw new IndexConfigException("table: " + info + " can't find field: " + s);
                }
            }
        }
        if (info.getDeleteField() != null && !allFields.contains(info.getDeleteField())) {
            info.clearDeleteField();
        }
        return handle;
    }

    private void verifyMasterAliasRepeat(TableQueryInfo tableQueryInfo, List<String> aliasList) {
        final Set<String> allField;
        DbTableDesc table = tableQueryInfo.getQueryCommon().getTable();
        try {
            allField = tableQueryInfo.getQueryHandler().getAllFields(table);
        } catch (ExecutionException e) {
            throw new IndexConfigException("get table: " + table + " fields have crash: " + e.getMessage(), e);
        }
        final Map<String, Integer> countMap = new HashMap<>();
        aliasList.forEach(v -> {
            if (allField.contains(v)) {
                throw new IndexConfigException(String.format("index table %s has exist %s, you need rename %s value",
                        table, v, ConfigKeyName.INDEX_TABLE_MASTER_ALIAS));
            }
            countMap.put(v, countMap.getOrDefault(v, 0) + 1);
        });
        StringBuilder sb = new StringBuilder();
        countMap.forEach((k, v) -> {
            if (v > 1) {
                sb.append(k).append(": ").append(v).append(' ');
            }
        });
        if (sb.length() > 0) {
            throw new IndexConfigException(String.format("index table %s config %s value has same value: %s",
                    table, ConfigKeyName.INDEX_TABLE_MASTER_ALIAS, sb));
        }
    }

    @Override
    protected DataSourceParserManager getDataSourceConfigFactory() {
        return dataSourceConfigFactory;
    }

    @Override
    protected IndexConfigParse getIndexConfigParse() {
        return indexConfParser;
    }
}
