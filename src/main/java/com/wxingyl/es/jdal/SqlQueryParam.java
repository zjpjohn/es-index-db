package com.wxingyl.es.jdal;

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

    public void setPrepareSql(PrepareSqlQuery prepareSql) {
        this.prepareSql = prepareSql;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public void setWhereField(String whereField) {
        this.whereField = whereField;
    }

    public void setCls(Class<T> cls) {
        this.cls = cls;
    }

    public void setWhereList(Collection<T> whereList) {
        this.whereList = whereList;
    }

    public PrepareSqlQuery getPrepareSql() {
        return prepareSql;
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
}
