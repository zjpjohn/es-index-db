package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.IndexSlaveResultMergeEnum;
import com.wxingyl.es.conf.index.DbTableConfigInfo;

/**
 * Created by xing on 15/8/27.
 * query data from table, the sql has common part, eg:
 *  SELECT XX, YY, ZZ FROM TABLE_NAME
 * so we can prepare create common part, no need to create every query
 */
public class SqlQueryCommon {
    /**
     * query table
     */
    private DbTableDesc table;

    /**
     * the field is primary key, its value should be unique in table
     */
    private String keyField;

    private String commonSql;

    private boolean containWhere;

    private int pageSize;

    private String orderBy;

    private String masterAlias;

    private IndexSlaveResultMergeEnum mergeType;

    public String getOrderBy() {
        return orderBy;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getCommonSql() {
        return commonSql;
    }

    public boolean isContainWhere() {
        return containWhere;
    }

    public DbTableDesc getTable() {
        return table;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    public IndexSlaveResultMergeEnum getMergeType() {
        return mergeType;
    }

    public String getKeyField() {
        return keyField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqlQueryCommon)) return false;

        SqlQueryCommon that = (SqlQueryCommon) o;

        return table.equals(that.table);

    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public String toString() {
        return "PrepareSqlQuery{" +
                "commonSql='" + commonSql + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", pageSize=" + pageSize +
                ", tableField=" + table +
                '}';
    }

    public static Build build() {
        return new Build();
    }

    public static class Build {

        private String commonFormatSql;

        private boolean containWhere;

        private String orderBy;

        public Build commonFormatSql(String commonFormatSql) {
            this.commonFormatSql = commonFormatSql;
            return this;
        }

        public boolean isContainWhere() {
            return containWhere;
        }

        public Build containWhere() {
            this.containWhere = true;
            return this;
        }

        public Build orderBy(String orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        public SqlQueryCommon build(DbTableConfigInfo tableInfo) {
            SqlQueryCommon sqlQueryCommon = new SqlQueryCommon();
            sqlQueryCommon.commonSql = commonFormatSql;
            sqlQueryCommon.containWhere = containWhere;
            sqlQueryCommon.orderBy = orderBy;
            sqlQueryCommon.pageSize = tableInfo.getPageSize();
            sqlQueryCommon.table = tableInfo.getTable();
            sqlQueryCommon.keyField = tableInfo.getRelationField();
            sqlQueryCommon.masterAlias = tableInfo.getMasterAlias();
            sqlQueryCommon.mergeType = tableInfo.getMergeType();
            return sqlQueryCommon;
        }
    }
}
