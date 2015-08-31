package com.wxingyl.es.jdal;

import com.wxingyl.es.conf.index.DbTableConfigInfo;

/**
 * Created by xing on 15/8/27.
 * query data from table, the sql has common part, eg:
 *  SELECT XX, YY, ZZ FROM TABLE_NAME
 * so we can prepare create common part, no need to create every query
 */
public class PrepareSqlQuery {

    private String tableName;

    private String commonFormatSql;

    private boolean containWhere;

    private int pageSize;

    private String orderBy;

    private String keyField;

    private String masterAlias;

    public String getKeyField() {
        return keyField;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getCommonFormatSql() {
        return commonFormatSql;
    }

    public boolean isContainWhere() {
        return containWhere;
    }

    public String getTableName() {
        return tableName;
    }

    public String getMasterAlias() {
        return masterAlias;
    }

    @Override
    public String toString() {
        return "PrepareSqlQuery{" +
                "commonFormatSql='" + commonFormatSql + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", pageSize=" + pageSize +
                ", keyField=" + keyField +
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

        public PrepareSqlQuery build(DbTableConfigInfo tableInfo) {
            PrepareSqlQuery prepareSqlQuery = new PrepareSqlQuery();
            prepareSqlQuery.commonFormatSql = commonFormatSql;
            prepareSqlQuery.containWhere = containWhere;
            prepareSqlQuery.orderBy = orderBy;
            prepareSqlQuery.pageSize = tableInfo.getPageSize();
            prepareSqlQuery.tableName = tableInfo.getTableName();
            prepareSqlQuery.keyField = tableInfo.getRelationField();
            prepareSqlQuery.masterAlias = tableInfo.getMasterAlias();
            return prepareSqlQuery;
        }
    }
}
