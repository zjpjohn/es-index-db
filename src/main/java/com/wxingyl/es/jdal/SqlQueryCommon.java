package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.DbTableConfigInfo;

/**
 * Created by xing on 15/8/27.
 * query data from table, the sql has common part, eg:
 *  SELECT XX, YY, ZZ FROM TABLE_NAME
 * so we can prepare create common part, no need to create every query
 */
public class SqlQueryCommon {
    /**
     * query table, the field is primary key, its value should be unique in table
     */
    private DbTableFieldDesc tableField;

    private String commonSql;

    private boolean containWhere;

    private int pageSize;

    private String orderBy;

    private String masterAlias;

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

    public DbTableFieldDesc getTableField() {
        return tableField;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SqlQueryCommon)) return false;

        SqlQueryCommon that = (SqlQueryCommon) o;

        return tableField.equals(that.tableField);

    }

    @Override
    public int hashCode() {
        return tableField.hashCode();
    }

    @Override
    public String toString() {
        return "PrepareSqlQuery{" +
                "commonSql='" + commonSql + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", pageSize=" + pageSize +
                ", tableField=" + tableField +
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
            sqlQueryCommon.tableField = new DbTableFieldDesc(tableInfo.getTable(), tableInfo.getRelationField());
            sqlQueryCommon.masterAlias = tableInfo.getMasterAlias();
            return sqlQueryCommon;
        }
    }
}
