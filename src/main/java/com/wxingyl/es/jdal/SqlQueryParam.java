package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.IndexTypeBean;
import org.apache.commons.dbutils.ResultSetHandler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/27.
 * sql query param
 */
public class SqlQueryParam {

    private PrepareSqlQuery prepareSql;
    /**
     * Note: query db with pageSize, page start from 0
     */
    private int page;

    private Collection keyValueList;

    private ResultSetHandler<List<Map<String, Object>>> rsh;

    public SqlQueryParam(IndexTypeBean.TableQuery tableQuery) {
        this(tableQuery, null);
    }

    public SqlQueryParam(IndexTypeBean.TableQuery tableQuery, Collection list) {
        prepareSql = tableQuery.getCommonSql();
        rsh = tableQuery.getRsh();
        keyValueList = list;
    }

    public PrepareSqlQuery getPrepareSql() {
        return prepareSql;
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
                "prepareSql=" + prepareSql +
                ", start=" + page +
                ", whereList=" + keyValueList +
                '}';
    }

}
