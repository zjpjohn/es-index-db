package com.wxingyl.es.index.db;

import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.db.query.SqlQueryOperator;
import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbutils.ResultSetHandler;

import java.util.*;

/**
 * Created by xing on 15/8/27.
 * sql query param
 */
public class SqlQueryParam {

    private SqlQueryCommon queryCommon;
    /**
     * Note: query db with pageSize, page start from 0
     */
    private int page;

    private QueryCondition queryCondition;

    private ResultSetHandler<List<Map<String, Object>>> rsh;

    public SqlQueryParam(TableQueryBean tableQuery, int startPage) {
        this(tableQuery, startPage, null);
    }

    public SqlQueryParam(TableQueryBean tableQuery, int startPage, Collection list) {
        queryCommon = tableQuery.getQueryCommon();
        rsh = tableQuery.getRsh();
        this.page = startPage;
        if (!CommonUtils.isEmpty(list)) {
            Set<String> values = new HashSet<>();
            for (Object o : list) {
                values.add(o.toString());
            }
            queryCondition = QueryCondition.buildList(queryCommon.getBaseInfo().getKeyField(), SqlQueryOperator.IN, values);
        }
    }

    public SqlQueryCommon getQueryCommon() {
        return queryCommon;
    }

    public QueryCondition getQueryCondition() {
        return queryCondition;
    }

    public void addPage() {
        page++;
    }

    public int getPage() {
        return page;
    }

    public ResultSetHandler<List<Map<String, Object>>> getRsh() {
        return rsh;
    }

    @Override
    public String toString() {
        return "SqlQueryParam{" +
                "queryCommon=" + queryCommon +
                ", start=" + page +
                '}';
    }

}
