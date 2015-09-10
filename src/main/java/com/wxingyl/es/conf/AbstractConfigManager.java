package com.wxingyl.es.conf;

import com.wxingyl.es.db.DataSourceBean;
import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.conf.ds.DataSourceParserManager;
import com.wxingyl.es.conf.index.IndexConfigParse;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.conf.index.TypeConfigInfo;
import com.wxingyl.es.db.result.ResultSetHandlerFactory;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Listener;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by xing on 15/9/8.
 * abstract config manager
 */
public abstract class AbstractConfigManager implements ConfigManager {

    /**
     * key: schema name, value: set DataSourceBean
     */
    private Map<String, Set<DataSourceBean>> dataSourceMap = new HashMap<>();
    /**
     * key: index name, value: set IndexTypeBean
     */
    private Map<String, Set<IndexTypeBean>> indexTypeMap = new HashMap<>();

    private List<Listener<Set<DataSourceBean>>> dataSourceListeners = new ArrayList<>();

    private List<Listener<Set<IndexTypeBean>>> indexTypeListeners = new ArrayList<>();

    private ResultSetHandlerFactory resultSetHandlerFactory;

    @Override
    public boolean addDataSourceConfigParser(DataSourceConfigParse parser) {
        return getDataSourceConfigFactory().addDataSourceConfigParser(parser);
    }

    @Override
    public boolean supportDbParse(String driverClassName) {
        return getDataSourceConfigFactory().supportParse(driverClassName);
    }

    @Override
    public void parseDataSource(String yamlFileName) {
        Map<String, Map<String, Object>> map = readYamlFile(yamlFileName);
        addDataSourceBean(getDataSourceConfigFactory().parseAll(map));
    }

    @Override
    public void parseDataSource(String configName, Map<String, Object> confMap) {
        addDataSourceBean(getDataSourceConfigFactory().parse(configName, confMap));
    }

    @Override
    public void parseIndexType(String yamlFileName) {
        Map<String, Map<String, Object>> map = readYamlFile(yamlFileName);
        if (map.get(ConfigKeyName.DS_DATA_SOURCE) != null) {
            parseDataSource("index-config", map.get(ConfigKeyName.DS_DATA_SOURCE));
            map.remove(ConfigKeyName.DS_DATA_SOURCE);
        }
        addIndexTypeBean(transformToIndexTypeBean(getIndexConfigParse().parseAll(map)));
    }

    @Override
    public void parseIndexType(String index, Map<String, Object> confMap) {
        addIndexTypeBean(transformToIndexTypeBean(getIndexConfigParse().parse(index, confMap)));
    }

    @Override
    public DataSourceBean findDataSourceBean(String schema, String urlAddress) {
        Objects.requireNonNull(schema);
        Set<DataSourceBean> set = dataSourceMap.get(schema);
        if (CommonUtils.isEmpty(set)) return null;
        for (DataSourceBean ds : set) {
            if (urlAddress == null || urlAddress.equalsIgnoreCase(ds.getUrlAddress())) {
                return ds;
            }
        }
        return null;
    }

    @Override
    public IndexTypeBean findIndexTypeBean(IndexTypeDesc type) {
        Set<IndexTypeBean> set = indexTypeMap.get(type.getIndex());
        if (CommonUtils.isEmpty(set)) return null;
        for (IndexTypeBean tb : set) {
            if (tb.getType().equals(type)) return tb;
        }
        return null;
    }

    @Override
    public Set<IndexTypeBean> findIndexTypeBean(String index) {
        return indexTypeMap.get(index);
    }

    @Override
    public boolean registerDataSourceListener(Listener<Set<DataSourceBean>> listener) {
        return !dataSourceListeners.contains(listener) && dataSourceListeners.add(listener);
    }

    @Override
    public boolean registerIndexTypeListener(Listener<Set<IndexTypeBean>> listener) {
        return !indexTypeListeners.contains(listener) && indexTypeListeners.add(listener);
    }

    @SuppressWarnings("unchecked")
    protected <T> Map<String, T> readYamlFile(String yamlFileName) {
        try (InputStream in = new FileInputStream(yamlFileName)) {
            Yaml yaml = new Yaml();
            return (Map<String, T>) yaml.load(in);
        } catch (IOException e) {
            throw new IndexConfigException("load config file: " + yamlFileName + " have IOException", e);
        }
    }

    protected void addIndexTypeBean(Set<IndexTypeBean> beanSet) {
        if (CommonUtils.isEmpty(beanSet)) return;
        beanSet.forEach(k -> {
            Set<IndexTypeBean> set = indexTypeMap.get(k.getType().getIndex());
            if (set == null) {
                indexTypeMap.put(k.getType().getIndex(), set = new HashSet<>());
            }
            set.add(k);
        });
        indexTypeListeners.forEach(l -> l.onChange(beanSet));
    }

    protected void addDataSourceBean(Set<DataSourceBean> beanSet) {
        if (CommonUtils.isEmpty(beanSet)) return;
        beanSet.forEach(k -> {
            Set<DataSourceBean> set = dataSourceMap.get(k.getSchema());
            if (set == null) {
                dataSourceMap.put(k.getSchema(), set = new HashSet<>());
            }
            set.add(k);
        });
        dataSourceListeners.forEach(l -> l.onChange(beanSet));
    }

    protected Map<String, Set<DataSourceBean>> getDataSourceMap() {
        return dataSourceMap;
    }

    protected Map<String, Set<IndexTypeBean>> getIndexTypeMap() {
        return indexTypeMap;
    }

    protected abstract DataSourceParserManager getDataSourceConfigFactory();

    protected abstract IndexConfigParse getIndexConfigParse();

    protected abstract Set<IndexTypeBean> transformToIndexTypeBean(Set<TypeConfigInfo> typeSet);
}
