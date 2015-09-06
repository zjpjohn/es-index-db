package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;
import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.*;
import java.util.stream.Collectors;

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
}
