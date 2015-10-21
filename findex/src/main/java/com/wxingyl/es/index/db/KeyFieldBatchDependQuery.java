package com.wxingyl.es.index.db;

import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.exception.IndexDocException;

import java.sql.SQLException;

/**
 * Created by xing on 15/10/21.
 * base table keyField query data, return depended table data
 */
public class KeyFieldBatchDependQuery extends AbstractDependQuery {

    public KeyFieldBatchDependQuery(TableQueryBean queryBean) {
        super(queryBean);
    }

    public DbQueryDependResult query(TableQueryResult tableResult) {
        try {
            DbQueryDependResult dependResult = new DbQueryDependResult(tableResult);
            slaveQuery(dependResult, queryBean.getSlaveQuery());
            return dependResult;
        } catch (SQLException e) {
            throw new IndexDocException("query slave table data have sqlException from: " + queryBean.getQueryCommon(), e);
        }
    }

}
