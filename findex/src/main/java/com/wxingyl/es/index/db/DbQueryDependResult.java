package com.wxingyl.es.index.db;

import com.wxingyl.es.db.result.TableQueryResult;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.*;

/**
 * Created by xing on 15/9/2.
 * query result depend handle
 */
public class DbQueryDependResult {

    private TableQueryResult tableQueryResult;

    private Multimap<String, DbQueryDependResult> slaveResult;

    public DbQueryDependResult(TableQueryResult tableQueryResult) {
        this.tableQueryResult = tableQueryResult;
    }

    public boolean isEmpty() {
        return tableQueryResult.isEmpty();
    }

    public boolean needContinue() {
        return tableQueryResult.needContinue();
    }

    public TableQueryResult getTableQueryResult() {
        return tableQueryResult;
    }

    public Multimap<String, DbQueryDependResult> getSlaveResult() {
        return slaveResult;
    }

    public Set<Object> getValuesForField(String field) {
        Set<Object> ret = new HashSet<>();
        for (Map<String, Object> v : tableQueryResult.getDbData()) {
            if (v.get(field) != null) {
                ret.add(v.get(field));
            }
        }
        return ret;
    }

    public void addSlaveResult(String masterField, DbQueryDependResult slaveRet) {
        if (slaveRet == this) {
            throw new IllegalArgumentException("can not add self");
        }
        if (slaveResult == null) {
            slaveResult = ArrayListMultimap.create();
        }
        slaveResult.put(masterField, slaveRet);
    }

    public List<TableQueryResult> getAllTableResult() {
        List<TableQueryResult> list = new LinkedList<>();
        addAllTableResult(list, this);
        return list;
    }

    private void addAllTableResult(List<TableQueryResult> list, DbQueryDependResult result) {
        list.add(result.tableQueryResult);
        if (result.slaveResult != null) {
            for (DbQueryDependResult v : result.slaveResult.values()) {
                addAllTableResult(list, v);
            }
        }
    }
}
