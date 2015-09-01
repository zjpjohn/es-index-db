package com.wxingyl.es.jdal;

import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.*;

/**
 * Created by xing on 15/8/30.
 * query db result
 */
public class DbQueryResult {

    private SqlQueryCommon sqlQuery;

    private List<Map<String, Object>> dbData;

    private Multimap<String, DbQueryResult> slaveResult;

    public DbQueryResult(SqlQueryCommon sqlQuery, List<Map<String, Object>> list) {
        this.sqlQuery = sqlQuery;
        dbData = list;
    }

    public Set<Object> getValuesForField(final String field) {
        Set<Object> set = new HashSet<>();
        for (Map<String, Object> v : dbData) {
            if (v.get(field) != null) {
                set.add(v.get(field));
            }
        }
        return set;
    }

    public boolean isEmpty() {
        return dbData.isEmpty();
    }

    public boolean needContinue() {
        return dbData.size() == sqlQuery.getPageSize();
    }

    public SqlQueryCommon getSqlQuery() {
        return sqlQuery;
    }

    public List<Map<String, Object>> getDbData() {
        return dbData;
    }

    public Multimap<String, DbQueryResult> getSlaveResult() {
        return slaveResult;
    }

    private Map<Object, List<Map<String, Object>>> groupAndRemoveByKeyField() {
        final Map<Object, List<Map<String, Object>>> ret = new HashMap<>();
        final String keyField = sqlQuery.getTableField().getField();
        dbData.forEach(v -> {
            Object obj = v.get(keyField);
            List<Map<String, Object>> list = ret.get(obj);
            if (list == null) {
                ret.put(obj, list = new LinkedList<>());
            }
            v.remove(keyField);
            list.add(v);
        });
        return ret;
    }

    public DbQueryResult addPageResult(DbQueryResult slaveRet) {
        if (!(slaveRet == null || this == slaveRet || slaveRet.isEmpty())) {
            dbData.addAll(slaveRet.dbData);
        }
        return this;
    }

    public void addSlaveResult(String masterField, DbQueryResult slaveRet) {
        if (slaveRet == this) {
            throw new IllegalArgumentException("can not add self");
        }
        if (slaveResult == null) {
            slaveResult = ArrayListMultimap.create();
        }
        slaveResult.put(masterField, slaveRet);
//
//        Map<Object, List<Map<String, Object>>> slaveMap = slaveRet.groupAndRemoveByKeyField();
//        String keyAlias = slaveRet.sqlQuery.getMasterAlias();
//        for (Map<String, Object> v : dbData) {
//            Object obj = v.get(masterField);
//            List<Map<String, Object>> list = slaveMap.get(obj);
//            if (list == null) continue;
//            v.put(keyAlias, list);
//        }
    }

}
