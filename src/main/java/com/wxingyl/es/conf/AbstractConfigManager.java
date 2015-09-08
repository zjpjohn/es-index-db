package com.wxingyl.es.conf;

import com.wxingyl.es.conf.ds.DataSourceBean;
import com.wxingyl.es.conf.ds.DataSourceConfigParse;
import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.exception.IndexConfigException;
import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.util.CommonUtils;
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
     * key: schema name, value: list DataSourceBean
     */
    private Map<String, Set<DataSourceBean>> dataSourceMap = new HashMap<>();
    /**
     * key: index name, value: list IndexTypeBean
     */
    private Map<String, Set<IndexTypeBean>> indexTypeMap = new HashMap<>();

    protected abstract DataSourceConfigParse getDataSourceConfigFactory();

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
        parseDataSource(readYamlFile(yamlFileName));
    }

    @Override
    public void parseIndexType(String yamlFileName) {
        parseIndexType(readYamlFile(yamlFileName));
    }

    @Override
    public DataSourceBean getDataSourceBean(DbTableDesc table) {
        if (dataSourceMap == null) return null;
        Collection<DataSourceBean> collection = dataSourceMap.get(table.getSchema());
        if (CommonUtils.isEmpty(collection)) return null;
        for (DataSourceBean ds : collection) {
            if (table.getUrlAddress() == null || table.getUrlAddress().equalsIgnoreCase(ds.getUrlAddress())) {
                return ds;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> readYamlFile(String yamlFileName) {
        try (InputStream in = new FileInputStream(yamlFileName)) {
            Yaml yaml = new Yaml();
            return (Map<String, Object>) yaml.load(in);
        } catch (IOException e) {
            throw new IndexConfigException("load config file: " + yamlFileName + " have IOException", e);
        }
    }

    protected boolean addIndexTypeBean(IndexTypeBean typeBean) {
        Set<IndexTypeBean> set = indexTypeMap.get(typeBean.getType().getIndex());
        if (set == null) {
            indexTypeMap.put(typeBean.getType().getIndex(), set = new HashSet<>());
        }
        return set.add(typeBean);
        //TODO index type config change, should notify some registers
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
        //TODO index type config change, should notify some registers
    }

    protected Map<String, Set<DataSourceBean>> getDataSourceMap() {
        return dataSourceMap;
    }

    protected Map<String, Set<IndexTypeBean>> getIndexTypeMap() {
        return indexTypeMap;
    }
}
