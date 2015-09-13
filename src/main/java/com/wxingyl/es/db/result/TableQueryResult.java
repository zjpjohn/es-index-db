package com.wxingyl.es.db.result;

import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.db.TableBaseInfo;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/30.
 * query db result
 */
public class TableQueryResult {

    private TableBaseInfo baseInfo;

    private int pageSize;
    /**
     * can not null
     */
    private List<Map<String, Object>> dbData;

    private TableQueryResult() {}

    public boolean isEmpty() {
        return dbData.isEmpty();
    }

    public boolean needContinue() {
        return dbData.size() == pageSize;
    }

    public List<Map<String, Object>> getDbData() {
        return dbData;
    }

    public TableBaseInfo getBaseInfo() {
        return baseInfo;
    }

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

        private List<Map<String, Object>> dbData;

        public Builder dbData(List<Map<String, Object>> dbData) {
            this.dbData = dbData;
            return this;
        }

        public TableQueryResult build(SqlQueryCommon common) {
            TableQueryResult ret = new TableQueryResult();
            ret.pageSize = common.getPageSize();
            ret.dbData = dbData;
            ret.baseInfo = common.getTableBaseInfo();
            return ret;
        }

    }

}
