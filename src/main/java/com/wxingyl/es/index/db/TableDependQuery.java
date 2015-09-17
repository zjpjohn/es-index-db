package com.wxingyl.es.index.db;

import com.wxingyl.es.db.query.TableQueryInfo;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.ImmutableMultimap;

import java.sql.SQLException;
import java.util.*;

/**
 * Created by xing on 15/8/31.
 * Note: thread unsafe
 */
public class TableDependQuery implements Iterator<DbQueryDependResult> {

    private SqlQueryParam masterParam;

    private SqlQueryHandle masterQueryHandler;

    private ImmutableMultimap<String, TableQueryInfo> slaveQuery;

    private boolean hasNext = true;

    private final int endPage;

    public TableDependQuery(TableQueryInfo masterTable, int startPage, int endPage) {
        masterParam = new SqlQueryParam(masterTable, startPage);
        masterQueryHandler = masterTable.getQueryHandler();
        slaveQuery = masterTable.getSlaveQuery();
        this.endPage = endPage;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    /**
     * query next
     */
    @Override
    public DbQueryDependResult next() {
        try {
            TableQueryResult queryResult = masterQueryHandler.query(masterParam);
            DbQueryDependResult masterResult = new DbQueryDependResult(queryResult);
            slaveQuery(masterResult, slaveQuery);
            masterParam.addPage();
            hasNext = masterResult.needContinue();
            if (hasNext && endPage > 0) {
                hasNext = masterParam.getPage() < endPage;
            }
            return masterResult;
        } catch (SQLException e) {
            hasNext = false;
            throw new IndexDocException("query data have sqlException from: " + masterParam, e);
        }

    }

    private void slaveQuery(DbQueryDependResult masterResult, ImmutableMultimap<String, TableQueryInfo> slaveMap) throws SQLException {
        if (masterResult == null || masterResult.isEmpty() || slaveMap == null) return;
        for (String field : slaveMap.keys()) {
            Set<Object> set = masterResult.getValuesForField(field);
            if (set.isEmpty()) continue;
            for (TableQueryInfo slaveTableQuery : slaveMap.get(field)) {
                TableQueryResult slaveRet;
                if (set.size() > slaveTableQuery.getQueryCommon().getPageSize()) {
                    slaveRet = groupQuery(set, slaveTableQuery);
                } else {
                    slaveRet = pageQuery(set, slaveTableQuery);
                }

                if (slaveRet == null || slaveRet.isEmpty()) continue;
                DbQueryDependResult slaveResult = new DbQueryDependResult(slaveRet);

                slaveQuery(slaveResult, slaveTableQuery.getSlaveQuery());

                masterResult.addSlaveResult(field, slaveResult);
            }
        }
    }

    private TableQueryResult pageQuery(Collection<Object> collection, TableQueryInfo queryInfo) throws SQLException {
        SqlQueryParam param = new SqlQueryParam(queryInfo, 0, collection);
        TableQueryResult ret = null;
        do {
            TableQueryResult result = queryInfo.getQueryHandler().query(param);
            ret = ret == null ? result : ret.addPageResult(result);
            param.addPage();
        } while (ret != null && ret.needContinue());
        return ret;
    }

    private TableQueryResult groupQuery(Set<Object> set, TableQueryInfo queryInfo) throws SQLException {
        TableQueryResult ret = null;
        for (List<Object> whereList : CommonUtils.groupList(set, queryInfo.getQueryCommon().getPageSize())) {
            TableQueryResult result = pageQuery(whereList, queryInfo);
            ret = ret == null ? result : ret.addPageResult(result);
        }
        return ret;
    }
}
