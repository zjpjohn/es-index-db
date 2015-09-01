package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.DataSourceBean;
import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.conf.ds.DataSourceParseFactory;
import com.wxingyl.es.conf.ds.MysqlDataSourceConfigParser;
import com.wxingyl.es.conf.index.*;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.ImmutableMultimap;
import org.elasticsearch.common.collect.ImmutableSetMultimap;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

/**
 * Created by xing on 15/8/24.
 * data source manager
 * should singleton obj
 * In the first place, we should parse datasource config ({@link #parseDataSource(String)}, {@link #parseDataSource(Map)}),
 * then parse index config ({@link #parseIndexType(String)}, {@link #parseIndexType(Map)}). If you index config had included
 * datasource config, this order is needless
 */
public class IndexDbConfigManager {

    private ConfigParse<TypeConfigInfo> indexConfParser;

    private DataSourceConfigParse dataSourceConfParser;

    /**
     * key: schema name, value: list DataSourceBean
     */
    private ImmutableMultimap<String, DataSourceBean> dataSourceMap;
    /**
     * key: index name, value: list IndexTypeBean
     */
    private ImmutableMultimap<String, IndexTypeBean> indexTypeMap;

    private BiConsumer<TableQueryInfo, List<String>> masterAliasVerify = this::verifyMasterAliasRepeat;

    /**
     * default add mysql parser
     */
    public IndexDbConfigManager() {
        indexConfParser = new IndexTypeConfigParser();
        dataSourceConfParser = new DataSourceParseFactory();
        dataSourceConfParser.addDataSourceConfigParser(new MysqlDataSourceConfigParser());
    }

    public DataSourceConfigParse getDataSourceConfParser() {
        return dataSourceConfParser;
    }

    @SuppressWarnings("unchecked")
    public void parseDataSource(String yamlFileName) {
        Map<String, Map<String, Object>> map = null;
        try (InputStream in = new FileInputStream(yamlFileName)) {
            Yaml yaml = new Yaml();
            map = (Map<String, Map<String, Object>>) yaml.load(in);
        } catch (IOException e) {
            throw new IndexConfigException("load config file: " + yamlFileName + " have IOException", e);
        }
        addDataSourceBean(dataSourceConfParser.parse(map.values()));
    }

    public void parseDataSource(Map<String, Object> confMap) {
        addDataSourceBean(dataSourceConfParser.parse(confMap));
    }

    @SuppressWarnings("unchecked")
    public void parseIndexType(String yamlFileName) {
        Map<String, Object> map = null;
        try (InputStream in = new FileInputStream(yamlFileName)) {
            Yaml yaml = new Yaml();
            map = (Map<String, Object>) yaml.load(in);
        } catch (IOException e) {
            throw new IndexConfigException("load config file: " + yamlFileName + " have IOException", e);
        }
        if (map.get(ConfigKeyName.DS_DATA_SOURCE) != null) {
            parseDataSource((Map<String, Object>) map.get(ConfigKeyName.DS_DATA_SOURCE));
            map.remove(ConfigKeyName.DS_DATA_SOURCE);
        }
        parseIndexType(map);
    }

    public void parseIndexType(Map<String, Object> confMap) {
        Set<TypeConfigInfo> typeSet = indexConfParser.parse(confMap);
        if (CommonUtils.isEmpty(typeSet)) return;
        ImmutableSetMultimap.Builder<String, IndexTypeBean> mapBuilder = ImmutableSetMultimap.builder();
        for (final TypeConfigInfo type : typeSet) {
            final IndexTypeBean.Builder builder = IndexTypeBean.build(type.getIndex(), type.getType());
            for (DbTableConfigInfo tableInfo : type.getTables()) {
                builder.addTableQuery(verifyTypeTableConfig(type, tableInfo), tableInfo);
            }
            mapBuilder.put(type.getIndex(), builder.build(type.getMasterTable(), masterAliasVerify));
        }
        if (indexTypeMap != null) mapBuilder.putAll(indexTypeMap);
        indexTypeMap = mapBuilder.build();
        //TODO index type config change, should notify some registers
    }

    private SqlQueryHandle verifyTypeTableConfig(TypeConfigInfo type, DbTableConfigInfo info) {
        DbTableDesc table = info.getTable();
        DataSourceBean dataSourceBean = findDataSourceBean(table);
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
        DbTableDesc table = tableQueryInfo.getQueryCommon().getTableField();
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

    private DataSourceBean findDataSourceBean(DbTableDesc table) {
        Collection<DataSourceBean> collection = dataSourceMap.get(table.getSchema());
        if (CommonUtils.isEmpty(collection)) return null;
        for (DataSourceBean ds : collection) {
            if (table.getUrlAddress() == null || table.getUrlAddress().equalsIgnoreCase(ds.getUrlAddress())) {
                return ds;
            }
        }
        return null;
    }

    private void addDataSourceBean(Set<DataSourceBean> beanSet) {
        if (CommonUtils.isEmpty(beanSet)) return;
        final ImmutableSetMultimap.Builder<String, DataSourceBean> builder = ImmutableSetMultimap.builder();
        if (dataSourceMap != null) builder.putAll(dataSourceMap);
        beanSet.forEach(k -> builder.put(k.getSchema(), k));
        dataSourceMap = builder.build();
    }
}
