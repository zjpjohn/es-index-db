package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.TableQueryInfo;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.SqlQueryParam;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by xing on 15/8/31.
 * Note: thread unsafe
 */
public class TableDependQuery implements Iterator<DbQueryResult> {

    private DbQueryResult queryResult;

    private SqlQueryParam masterParam;

    private SqlQueryHandle masterQueryHandler;

    private Map<TableQueryInfo, String> slaveQuery;

    public TableDependQuery(TableQueryInfo masterTable) {
        masterParam = new SqlQueryParam(masterTable);
        masterQueryHandler = masterTable.getQueryHandler();
        slaveQuery = masterTable.getSlaveQuery();
    }

    @Override
    public boolean hasNext() {
        return masterParam.getPage() == 0 || queryResult.needContinue();
    }

    /**
     * query next
     */
    @Override
    public DbQueryResult next() {
        try {
            queryResult = masterQueryHandler.query(masterParam);
            fillSlaveData();
        } catch (SQLException e) {
            throw new IndexDocException("query data have sqlException from: " + masterParam, e);
        }
        masterParam.addPage();
        return null;
    }

    private void fillSlaveData() throws SQLException {
        if (slaveQuery == null || queryResult.isEmpty()) return;
        slaveQuery(queryResult, slaveQuery);
    }

    private void slaveQuery(DbQueryResult masterResult, Map<TableQueryInfo, String> slaveMap) throws SQLException {
        for (Map.Entry<TableQueryInfo, String> e : slaveMap.entrySet()) {
            Set<Object> set = masterResult.getValuesForField(e.getValue());
            if (set.isEmpty()) continue;
            TableQueryInfo slaveTableQuery = e.getKey();
            DbQueryResult slaveRet;
            if (set.size() > slaveTableQuery.getQueryCommon().getPageSize()) {
                slaveRet = groupQuery(set, slaveTableQuery);
            } else {
                slaveRet = pageQuery(set, slaveTableQuery);
            }

            if (slaveRet == null || slaveRet.isEmpty()) continue;

            if (slaveTableQuery.getSlaveQuery() != null ) {
                slaveQuery(slaveRet, slaveTableQuery.getSlaveQuery());
            }
            masterResult.addSlaveResult(e.getValue(), slaveRet);
        }
    }

    private DbQueryResult pageQuery(Collection<Object> collection, TableQueryInfo queryInfo) throws SQLException {
        SqlQueryParam param = new SqlQueryParam(queryInfo, collection);
        DbQueryResult ret = null;
        do {
            DbQueryResult result = queryInfo.getQueryHandler().query(param);
            ret = ret == null ? result : ret.addPageResult(result);
            param.addPage();
        } while (ret != null && ret.needContinue());
        return ret;
    }

    private DbQueryResult groupQuery(Set<Object> set, TableQueryInfo queryInfo) throws SQLException {
        DbQueryResult ret = null;
        for (List<Object> whereList : CommonUtils.groupList(set, queryInfo.getQueryCommon().getPageSize())) {
            DbQueryResult result = pageQuery(whereList, queryInfo);
            ret = ret == null ? result : ret.addPageResult(result);
        }
        return ret;
    }
}
