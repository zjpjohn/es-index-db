package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.SqlQueryParam;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/31.
 * Note: thread unsafe
 */
public class TableDependQuery implements Iterator<DbQueryResult> {

    private DbQueryResult queryResult;

    private SqlQueryParam masterParam;

    private SqlQueryHandle masterQueryHandler;

    private Map<IndexTypeBean.TableQuery, String> slaveQuery;
    /**
     * Note: page start 0
     */
    private int page;

    public TableDependQuery(IndexTypeBean.TableQuery masterTable) {
        masterParam = new SqlQueryParam(masterTable);
        masterQueryHandler = masterTable.getQueryHandler();
        slaveQuery = masterTable.getSlaveQuery();
    }

    @Override
    public boolean hasNext() {
        return page == 0 || queryResult.needContinue();
    }

    /**
     * query next
     */
    @Override
    public DbQueryResult next() {
        masterParam.setPage(page);
        try {
            queryResult = masterQueryHandler.query(masterParam);
            fillSlaveData();
        } catch (SQLException e) {
            throw new IndexDocException("query data have sqlException from: " + masterParam, e);
        }
        page++;
        return null;
    }

    private void fillSlaveData() throws SQLException {
        if (slaveQuery == null || queryResult.isEmpty()) return;
        slaveQuery(queryResult, slaveQuery);
    }

    private void slaveQuery(DbQueryResult masterResult, Map<IndexTypeBean.TableQuery, String> slaveMap) throws SQLException {
        for (Map.Entry<IndexTypeBean.TableQuery, String> e : slaveMap.entrySet()) {
            Set<Object> set = masterResult.getValuesForField(e.getValue());
            if (set.isEmpty()) continue;
            IndexTypeBean.TableQuery slaveTableQuery = e.getKey();
            SqlQueryParam param = new SqlQueryParam(slaveTableQuery, set);
            DbQueryResult slaveRet = slaveTableQuery.getQueryHandler().query(param);
            if (slaveRet.isEmpty()) continue;
            if (slaveTableQuery.getSlaveQuery() != null ) {
                slaveQuery(slaveRet, slaveTableQuery.getSlaveQuery());
            }
            masterResult.addSlaveResult(e.getValue(), slaveRet);
        }
    }

    public static TableDependQuery build(IndexTypeBean.TableQuery masterTable) {
        return new TableDependQuery(masterTable);
    }

}
