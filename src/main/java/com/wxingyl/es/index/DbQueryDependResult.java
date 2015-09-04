package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.HashSet;
import java.util.Map;
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

    public TableQueryResult getTableQueryResult() {
        return tableQueryResult;
    }

    public boolean isEmpty() {
        return tableQueryResult.isEmpty();
    }

    public boolean needContinue() {
        return tableQueryResult.needContinue();
    }

    public Multimap<String, DbQueryDependResult> getSlaveResult() {
        return slaveResult;
    }

    public Set<Object> getValuesForField(String field) {
        Set<Object> set = new HashSet<>();
        for (Map<String, Object> v : tableQueryResult.getDbData()) {
            if (v.get(field) != null) {
                set.add(v.get(field));
            }
        }
        return set;
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
}
