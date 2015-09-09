package com.wxingyl.es.db.query;

import com.wxingyl.es.db.DbTableDesc;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 * Created by xing on 15/9/8.
 * query param, self user, create index no use
 */
public class BaseQueryParam {

    private DbTableDesc table;

    private Map<String, Object> where;

    private Map<String, SortOrder> orderBy;

    private Set<String> fields;

    private Tuple<Integer, Integer> page;

    /**
     * @param page start 0, not 1
     * @param pageSize a page size
     */
    public void setPage(int page, int pageSize) {
        this.page = Tuple.tuple(page * pageSize, pageSize);
    }

    public void setTable(DbTableDesc table) {
        this.table = table;
    }

    public Object addWhere(String field, Object condition) {
        if (where == null) where = new HashMap<>();
        return where.put(field, condition);
    }

    public boolean addField(String... fields) {
        if (this.fields == null) this.fields = new HashSet<>();
        return Collections.addAll(this.fields, fields);
    }

    public SortOrder addOrder(String field, SortOrder order) {
        if (orderBy == null) orderBy = new HashMap<>();
        return orderBy.put(field, order);
    }

    public Set<String> getFields() {
        return fields;
    }

    public Map<String, SortOrder> getOrderBy() {
        return orderBy;
    }

    public Tuple<Integer, Integer> getPage() {
        return page;
    }

    public DbTableDesc getTable() {
        return table;
    }

    public Map<String, Object> getWhere() {
        return where;
    }
}
