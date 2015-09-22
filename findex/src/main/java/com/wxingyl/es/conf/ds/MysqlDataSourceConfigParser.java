package com.wxingyl.es.conf.ds;

import com.wxingyl.es.db.query.MysqlQueryHandler;
import com.wxingyl.es.db.query.SqlQueryHandle;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;

import javax.sql.DataSource;

/**
 * Created by xing on 15/8/17.
 * default mysql parse
 */
public class MysqlDataSourceConfigParser extends AbstractDataSourceConfigParser {


    @Override
    protected DataSource createDataSource(String url, String userName, String password) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(getSupportDriverClassName());
        dataSource.setUrl(url);
        dataSource.setUsername(userName);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Override
    protected String getSupportDriverClassName() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    protected SqlQueryHandle createSqlQueryHandler(DataSource dataSource) {
        return new MysqlQueryHandler(dataSource);
    }

    @Override
    protected Tuple<String, String> parseJdbcInfo(String jdbcUrl) {
        final int startIndex = jdbcUrl.indexOf("jdbc:mysql://") + "jdbc:mysql://".length();
        int endIndex, index = jdbcUrl.indexOf('/', startIndex);
        String schema = null;
        if (index > 0) {
            endIndex = index;
            index++;
            int schemaIndex = (schemaIndex = jdbcUrl.indexOf('?', index)) > 0 ? schemaIndex : jdbcUrl.length();
            if (index < schemaIndex) schema = jdbcUrl.substring(index, schemaIndex);
        } else if ((index = jdbcUrl.indexOf('?', startIndex)) > 0) {
            endIndex = index;
        } else {
            endIndex = jdbcUrl.length();
        }
        String ipAddress = jdbcUrl.substring(startIndex, endIndex);
        if (jdbcUrl.indexOf(':', startIndex) < 0) ipAddress += ":3306";
        return Tuple.tuple(ipAddress, schema);
    }
}
