package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigKeyName;
import com.wxingyl.es.exception.DataSourceConfigException;
import com.wxingyl.es.jdal.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/17.
 * single type db parser
 */
public abstract class AbstractDataSourceConfigParser implements DataSourceConfigParse {

    @Override
    public boolean supportParse(String driverClassName) {
        return driverClassName.equalsIgnoreCase(getSupportDriverClassName());
    }

    @Override
    public boolean addDataSourceConfigParser(DataSourceConfigParse parser) {
        throw new UnsupportedOperationException("Concrete implementation DataSourceConfigParse can't add parser");
    }

    @Override
    public boolean removeDataSourceConfigParser(DataSourceConfigParse parser) {
        throw new UnsupportedOperationException("Concrete implementation DataSourceConfigParse can't remove parser");
    }

    /**
     * parse single type db
     * @param schemaConf one of schemas config value, it contain url, username, password or db_names
     * @return DataSourceBean set, a schema have an obj
     */
    @Override
    public Set<DataSourceBean> parse(Map<String, Object> schemaConf) {
        final Set<DataSourceBean> ret = new HashSet<>();
        String url = CommonUtils.getStringVal(schemaConf, ConfigKeyName.DS_URL);
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(getSupportDriverClassName());
        dataSource.setUrl(url);
        dataSource.setUsername(CommonUtils.getStringVal(schemaConf, ConfigKeyName.DS_USERNAME));
        dataSource.setPassword(CommonUtils.getStringVal(schemaConf, ConfigKeyName.DS_PASSWORD));
        List<String> dbNames = CommonUtils.getList(schemaConf, ConfigKeyName.DS_DB_NAMES);
        final Tuple<String, String> jdbcInfo = parseJdbcInfo(url);
        if (dbNames == null && jdbcInfo.v2() == null) {
            throw new DataSourceConfigException("datasource config can't find schema for jdbc url: " + url );
        }
        SqlQueryHandle sqlQueryHandle = createSqlQueryHandler(dataSource);
        if (dbNames != null) {
            dbNames.forEach(v -> ret.add(DataSourceBean.build(jdbcInfo.v1(), v)
                    .queryHandle(sqlQueryHandle)
                    .build()));
        } else {
            ret.add(DataSourceBean.build(jdbcInfo.v1(), jdbcInfo.v2())
                    .queryHandle(sqlQueryHandle)
                    .build());
        }
        return ret;
    }

    protected abstract String getSupportDriverClassName();

    protected abstract SqlQueryHandle createSqlQueryHandler(BasicDataSource dataSource);

    /**
     * parse jdbc url, return ip address and schema name
     * @param jdbcUrl jdbc url
     * @return v1: ip address, include ip, port
     *         v2: if there is schema name, return
     */
    protected abstract Tuple<String, String> parseJdbcInfo(String jdbcUrl);

    @Override
    public int hashCode() {
        return getSupportDriverClassName().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (obj instanceof AbstractDataSourceConfigParser) {
            AbstractDataSourceConfigParser parser = (AbstractDataSourceConfigParser) obj;
            return getSupportDriverClassName().equals(parser.getSupportDriverClassName());
        } else {
            return false;
        }
    }
}
