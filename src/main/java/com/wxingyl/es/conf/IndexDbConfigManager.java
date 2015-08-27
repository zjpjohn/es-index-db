package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.DataSourceBean;
import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.conf.ds.DataSourceParseFactory;
import com.wxingyl.es.conf.ds.MysqlDataSourceConfigParser;
import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.conf.index.IndexTypeConfigParser;
import com.wxingyl.es.conf.index.TypeConfigInfo;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.ImmutableMultimap;
import org.elasticsearch.common.collect.ImmutableSetMultimap;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

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

    private DataSourceConfigParse dsConfParser;

    /**
     * key: schema name, value: list DataSourceBean
     */
    private ImmutableMultimap<String, DataSourceBean> dataSourceMap;
    /**
     * key: index name, value: list IndexTypeBean
     */
    private ImmutableMultimap<String, IndexTypeBean> indexTypeMap;

    /**
     * default add mysql parser
     */
    public IndexDbConfigManager() {
        indexConfParser = new IndexTypeConfigParser();
        dsConfParser = new DataSourceParseFactory();
        dsConfParser.addDataSourceConfigParser(new MysqlDataSourceConfigParser());
    }

    public DataSourceConfigParse getDsConfParser() {
        return dsConfParser;
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
        addDataSourceBean(dsConfParser.parse(map.values()));
    }

    public void parseDataSource(Map<String, Object> confMap) {
        addDataSourceBean(dsConfParser.parse(confMap));
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
        final ImmutableSetMultimap.Builder<String, IndexTypeBean> mapBuilder = ImmutableSetMultimap.builder();
        for (final TypeConfigInfo type : typeSet) {
            final IndexTypeBean.Build build = IndexTypeBean.build(type.getIndex(), type.getType());
            type.getTables().forEach(v -> build.addTableQuery(verifyTypeTableConfig(type, v), v));
            mapBuilder.put(type.getIndex(), build.build(type.getMasterTable()));
        }
        if (indexTypeMap != null) mapBuilder.putAll(indexTypeMap);
        indexTypeMap = mapBuilder.build();
        //TODO index type config change, should notify some registers
    }

    private SqlQueryHandle verifyTypeTableConfig(TypeConfigInfo type, DbTableConfigInfo info) {
        String schema = info.getSchema();
        DataSourceBean dataSourceBean = findDataSourceBean(schema, info.getDbAddress());
        if (dataSourceBean == null) {
            throw new IndexConfigException("Index config: " + type + ", schema: " + schema + ", dbAddress: "
                    + info.getDbAddress() + " can't find datasource config");
        }
        SqlQueryHandle handle = dataSourceBean.getQueryHandle();
        String table = info.getTableName();
        Set<String> allTables, allFields;
        try {
            allTables = handle.getAllTables(schema);
            allFields = handle.getAllFields(schema, table);
        } catch (Exception e) {
            throw new IndexConfigException("get " + info + " tables and fields have crash: " + e.getMessage(), e);
        }
        if (!allTables.contains(table)) {
            throw new IndexConfigException("can't find table: " + info);
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

    private DataSourceBean findDataSourceBean(String schemaName, final String dbAddress) {
        Collection<DataSourceBean> collection = dataSourceMap.get(schemaName);
        if (CommonUtils.isEmpty(collection)) return null;
        for (DataSourceBean ds : collection) {
            if (dbAddress == null || dbAddress.equalsIgnoreCase(ds.getUrlAddress())) {
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
