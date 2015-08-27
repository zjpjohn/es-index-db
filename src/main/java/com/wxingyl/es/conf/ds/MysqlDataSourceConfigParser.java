package com.wxingyl.es.conf.ds;

import com.wxingyl.es.jdal.handle.MysqlQueryHandler;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;

/**
 * Created by xing on 15/8/17.
 * default mysql parse
 */
public class MysqlDataSourceConfigParser extends AbstractDataSourceConfigParser {


    @Override
    protected String getSupportDriverClassName() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    protected SqlQueryHandle createSqlQueryHandler(BasicDataSource dataSource) {
        return new MysqlQueryHandler(dataSource);
    }

    @Override
    protected Tuple<String, String> parseJdbcInfo(String jdbcUrl) {
        int ipStartIndex = jdbcUrl.indexOf("jdbc:mysql://") + "jdbc:mysql://".length();
        int endIndex;
        int index = jdbcUrl.indexOf('/', ipStartIndex);
        String schema = null;
        if (index > 0) {
            endIndex = index;
            index++;
            int schemaIndex = (schemaIndex = jdbcUrl.indexOf('?', index)) > 0 ? schemaIndex : jdbcUrl.length();
            if (index < schemaIndex) schema = jdbcUrl.substring(index, schemaIndex);
        } else if ((index = jdbcUrl.indexOf('?', ipStartIndex)) > 0) {
            endIndex = index;
        } else {
            endIndex = jdbcUrl.length();
        }
        String ipAddress = jdbcUrl.substring(ipStartIndex, endIndex);
        if (jdbcUrl.indexOf(':', ipStartIndex) < 0) ipAddress += ":3306";
        return Tuple.tuple(ipAddress, schema);
    }
}
