package com.wxingyl.es.conf.ds;

import com.wxingyl.es.conf.ConfigKeyName;
import com.wxingyl.es.exception.DataSourceConfigException;
import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
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

    /**
     * parse single type db
     * @param config one of schemas config value, it contain url, username, password or db_names
     * @return DataSourceBean set, a schema have an obj
     */
    @Override
    public Set<DataSourceBean> parse(String configName, Map<String, Object> config) {
        final Set<DataSourceBean> ret = new HashSet<>();
        String url = CommonUtils.getStringVal(config, ConfigKeyName.DS_URL);
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(getSupportDriverClassName());
        dataSource.setUrl(url);
        dataSource.setUsername(CommonUtils.getStringVal(config, ConfigKeyName.DS_USERNAME));
        dataSource.setPassword(CommonUtils.getStringVal(config, ConfigKeyName.DS_PASSWORD));
        List<String> dbNames = CommonUtils.getList(config, ConfigKeyName.DS_DB_NAMES);
        Tuple<String, String> jdbcInfo = parseJdbcInfo(url);
        if (dbNames == null && jdbcInfo.v2() == null) {
            throw new DataSourceConfigException("datasource config: " + configName + " can't find schema for jdbc url: " + url );
        }
        SqlQueryHandle sqlQueryHandle = createSqlQueryHandler(dataSource, config);
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

    @SuppressWarnings("unchecked")
    private SqlQueryHandle createSqlQueryHandler(BasicDataSource dataSource, Map<String, Object> config) {
        String className = CommonUtils.getStringVal(config, ConfigKeyName.DS_QUERY_HANDLE_CLS);
        if (className == null) return createSqlQueryHandler(dataSource);
        try {
            Class cls = Class.forName(className);
            if (!SqlQueryHandle.class.isAssignableFrom(cls)) {
                throw new DataSourceConfigException("config " + ConfigKeyName.DS_QUERY_HANDLE_CLS + ": " + className
                        + " is not implement " + SqlQueryHandle.class);
            }
            Constructor constructor = cls.getConstructor(DataSource.class);
            return (SqlQueryHandle) constructor.newInstance(dataSource);
        } catch (Exception e) {
            throw new DataSourceConfigException("config " + ConfigKeyName.DS_QUERY_HANDLE_CLS + ": " + className
                    + " create failed", e);
        }
    }

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
