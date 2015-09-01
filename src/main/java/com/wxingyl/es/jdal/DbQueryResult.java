package com.wxingyl.es.jdal;

import java.util.*;

/**
 * Created by xing on 15/8/30.
 * query db result
 */
public class DbQueryResult {

    private PrepareSqlQuery sqlQuery;

    private List<Map<String, Object>> dbData;

    public DbQueryResult(PrepareSqlQuery sqlQuery, List<Map<String, Object>> list) {
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

    private Map<Object, List<Map<String, Object>>> groupAndRemoveByKeyField() {
        final Map<Object, List<Map<String, Object>>> ret = new HashMap<>();
        final String keyField = sqlQuery.getKeyField();
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

    public void addSlaveResult(String masterField, DbQueryResult slaveRet) {
        if (slaveRet == this) {
            throw new IllegalArgumentException("can not add self");
        }
        Map<Object, List<Map<String, Object>>> slaveMap = slaveRet.groupAndRemoveByKeyField();
        String keyAlias = slaveRet.sqlQuery.getMasterAlias();
        for (Map<String, Object> v : dbData) {
            Object obj = v.get(masterField);
            List<Map<String, Object>> list = slaveMap.get(obj);
            if (list == null) continue;
            v.put(keyAlias, list);
        }
    }

}
