package com.wxingyl.es.conf.ds;

import com.wxingyl.es.jdal.SqlQueryHandle;
import org.apache.commons.dbcp2.BasicDataSource;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/25.
 * default oracle parse
 */
public class OracleDataSourceConfigParser extends AbstractDataSourceConfigParser {

    @Override
    protected String getSupportDriverClassName() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    protected SqlQueryHandle createSqlQueryHandler(BasicDataSource dataSource) {
        return null;
    }

    @Override
    protected Tuple<String, String> parseJdbcInfo(String jdbcUrl) {
        return null;
    }

}
