package com.wxingyl.es.index.db;

import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.exception.IndexDocException;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by xing on 15/8/31.
 * Note: thread unsafe
 */
public class TableDependQuery extends AbstractDependQuery implements Iterator<DbQueryDependResult> {

    private SqlQueryParam masterParam;

    private boolean hasNext = true;

    private final int endPage;

    public TableDependQuery(TableQueryBean masterTable, int startPage, int endPage) {
        super(masterTable);
        masterParam = new SqlQueryParam(masterTable, startPage);
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
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        try {
            TableQueryResult queryResult = queryBean.getQueryHandler().query(masterParam);
            DbQueryDependResult masterResult = new DbQueryDependResult(queryResult);
            slaveQuery(masterResult, queryBean.getSlaveQuery());
            masterParam.addPage();
            if (!masterResult.needContinue() || (endPage > 0 && masterParam.getPage() >= endPage)) {
                hasNext = false;
            }
            return masterResult;
        } catch (SQLException e) {
            hasNext = false;
            throw new IndexDocException("query data have sqlException from: " + masterParam, e);
        }

    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

}
