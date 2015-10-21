package com.wxingyl.es.index.db;

import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.ImmutableMultimap;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created by xing on 15/10/21.
 * abstract depend query
 */
public abstract class AbstractDependQuery {

    protected final TableQueryBean queryBean;

    public AbstractDependQuery(TableQueryBean queryBean) {
        this.queryBean = queryBean;
    }

    protected void slaveQuery(DbQueryDependResult masterResult, ImmutableMultimap<String, TableQueryBean> slaveMap) throws SQLException {
        if (masterResult == null || masterResult.isEmpty() || slaveMap == null) return;
        for (String field : slaveMap.keys()) {
            Set<Object> set = masterResult.getValuesForField(field);
            if (set.isEmpty()) continue;
            for (TableQueryBean slaveTableQuery : slaveMap.get(field)) {
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

    private TableQueryResult pageQuery(Collection<Object> collection, TableQueryBean queryInfo) throws SQLException {
        SqlQueryParam param = new SqlQueryParam(queryInfo, 0, collection);
        TableQueryResult ret = null;
        do {
            TableQueryResult result = queryInfo.getQueryHandler().query(param);
            ret = ret == null ? result : ret.addPageResult(result);
            param.addPage();
        } while (ret != null && ret.needContinue());
        return ret;
    }

    private TableQueryResult groupQuery(Set<Object> set, TableQueryBean queryInfo) throws SQLException {
        TableQueryResult ret = null;
        for (List<Object> whereList : CommonUtils.groupList(set, queryInfo.getQueryCommon().getPageSize())) {
            TableQueryResult result = pageQuery(whereList, queryInfo);
            ret = ret == null ? result : ret.addPageResult(result);
        }
        return ret;
    }

}
