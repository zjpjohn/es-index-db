package com.wxingyl.es.index.db;

import com.wxingyl.es.db.result.TableQueryResult;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
        return tableQueryResult.getDbData().stream().filter(v -> v.get(field) != null)
                .collect(HashSet::new, (c, v) -> c.add(v.get(field)), Set::addAll);
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
            result.slaveResult.values().forEach(v -> addAllTableResult(list, v));
        }
    }
}
