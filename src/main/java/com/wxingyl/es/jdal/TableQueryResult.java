package com.wxingyl.es.jdal;

import org.elasticsearch.common.collect.ArrayListMultimap;
import org.elasticsearch.common.collect.Multimap;

import java.util.*;

/**
 * Created by xing on 15/8/30.
 * query db result
 */
public class TableQueryResult {

    private int pageSize;

    private DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;

    /**
     * can not null
     */
    private List<Map<String, Object>> dbData;

    private TableQueryResult() {}

    public int getPageSize() {
        return pageSize;
    }

    public DbTableDesc getTable() {
        return table;
    }

    public String getKeyField() {
        return keyField;
    }

    public boolean isEmpty() {
        return dbData.isEmpty();
    }

    public boolean needContinue() {
        return dbData.size() == pageSize;
    }

    public List<Map<String, Object>> getDbData() {
        return dbData;
    }

//    private Map<Object, List<Map<String, Object>>> groupAndRemoveByKeyField() {
//        final Map<Object, List<Map<String, Object>>> ret = new HashMap<>();
//        final String keyField = sqlQuery.getKeyField();
//        dbData.forEach(v -> {
//            Object obj = v.get(keyField);
//            List<Map<String, Object>> list = ret.get(obj);
//            if (list == null) {
//                ret.put(obj, list = new LinkedList<>());
//            }
//            v.remove(keyField);
//            list.add(v);
//        });
//        return ret;
//    }

    public TableQueryResult addPageResult(TableQueryResult slaveRet) {
        if (!(slaveRet == null || this == slaveRet || slaveRet.isEmpty())) {
            dbData.addAll(slaveRet.dbData);
        }
        return this;
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private int pageSize;

        private DbTableDesc table;

        private String keyField;

        private List<Map<String, Object>> dbData;

        public Builder sqlQueryCommon(SqlQueryCommon common) {
            pageSize = common.getPageSize();
            table = common.getTable();
            keyField = common.getKeyField();
            return this;
        }

        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder keyField(String keyField) {
            this.keyField = keyField;
            return this;
        }
        public Builder table(DbTableDesc table) {
            this.table = table;
            return this;
        }

        public Builder dbData(List<Map<String, Object>> dbData) {
            this.dbData = dbData;
            return this;
        }

        public TableQueryResult build() {
            TableQueryResult ret = new TableQueryResult();
            ret.pageSize = pageSize;
            ret.table = table;
            ret.keyField = keyField;
            ret.dbData = dbData;
            return ret;
        }

    }

}
