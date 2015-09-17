package com.wxingyl.es.conf.ds;

import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.db.query.SqlServerQueryHandler;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lang3.StringUtils;

import javax.sql.DataSource;

/**
 * Created by xing on 15/8/24.
 * default sqlserver parse
 */
public class SqlServerDataSourceConfigParser extends AbstractDataSourceConfigParser {

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
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    protected SqlQueryHandle createSqlQueryHandler(DataSource dataSource) {
        return new SqlServerQueryHandler(dataSource);
    }

    @Override
    protected Tuple<String, String> parseJdbcInfo(String jdbcUrl) {
        final int startIndex = jdbcUrl.indexOf("jdbc:sqlserver://") + "jdbc:sqlserver://".length();
        int endIndex, index = jdbcUrl.indexOf(';', startIndex);
        String schemaName = null;
        if (index > 0) {
            endIndex = index;
            index++;
            schemaName = getSchemaName(jdbcUrl.substring(index));
        } else {
            endIndex = jdbcUrl.length();
        }
        String ipPort = jdbcUrl.substring(startIndex, endIndex);
        if (ipPort.indexOf(':') < 0) ipPort += ":1433";
        return Tuple.tuple(ipPort, schemaName);
    }

    private String getSchemaName(String jdbcUrlArgs) {
        String[] array = StringUtils.split(jdbcUrlArgs, ';');
        if (array == null || array.length == 0) return null;
        for (String s : array) {
            String[] arr = s.split("=");
            if (arr[0].equalsIgnoreCase("DatabaseName")) return arr[1];
        }
        return null;
    }

}
