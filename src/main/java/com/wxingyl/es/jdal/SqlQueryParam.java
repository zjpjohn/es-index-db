package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.TableQueryInfo;
import org.apache.commons.dbutils.ResultSetHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    private Collection keyValueList;

    private ResultSetHandler<List<Map<String, Object>>> rsh;

    public SqlQueryParam(TableQueryInfo tableQuery) {
        this(tableQuery, null);
    }

    public SqlQueryParam(TableQueryInfo tableQuery, Collection list) {
        queryCommon = tableQuery.getQueryCommon();
        rsh = tableQuery.getRsh();
        keyValueList = list;
    }

    public SqlQueryCommon getQueryCommon() {
        return queryCommon;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPage() {
        return page;
    }

    public Collection getKeyValueList() {
        return keyValueList;
    }

    public ResultSetHandler<List<Map<String, Object>>> getRsh() {
        return rsh;
    }

    @Override
    public String toString() {
        return "SqlQueryParam{" +
                "queryCommon=" + queryCommon +
                ", start=" + page +
                ", whereList=" + keyValueList +
                '}';
    }

}
