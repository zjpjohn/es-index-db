package com.wxingyl.es.conf.ds;

import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.db.query.SqlServerQueryHandler;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lang3.StringUtils;

/**
 * Created by xing on 15/8/24.
 * default sqlserver parse
 */
public class SqlServerDataSourceConfigParser extends AbstractDataSourceConfigParser {

    @Override
    protected String getSupportDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    protected SqlQueryHandle createSqlQueryHandler(BasicDataSource dataSource) {
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
