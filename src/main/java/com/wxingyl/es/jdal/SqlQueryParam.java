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
public class SqlQueryParam<T> {

    private PrepareSqlQuery prepareSql;

    private int start;

    private String whereField;

    private Class<T> cls;

    private Collection<T> whereList;

    private ResultSetHandler<List<Map<String, Object>>> handler;

    public PrepareSqlQuery getPrepareSql() {
        return prepareSql;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getStart() {
        return start;
    }

    public String getWhereField() {
        return whereField;
    }

    public Class<T> getCls() {
        return cls;
    }

    public Collection<T> getWhereList() {
        return whereList;
    }

    @Override
    public String toString() {
        return "SqlQueryParam{" +
                "prepareSql=" + prepareSql +
                ", start=" + start +
                ", whereField='" + whereField + '\'' +
                ", whereList=" + whereList +
                '}';
    }

    public static SqlQueryParam<Void> createMasterQueryParam(IndexTypeBean.TableQuery tableQuery) {
        SqlQueryParam<Void> param = new SqlQueryParam<>();
        param.prepareSql = tableQuery.getCommonSql();
        param.handler = tableQuery.getRsh();
        return param;
    }
}
