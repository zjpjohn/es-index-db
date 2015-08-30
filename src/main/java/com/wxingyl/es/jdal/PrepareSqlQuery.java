package com.wxingyl.es.jdal;

/**
 * Created by xing on 15/8/27.
 * query data from table, the sql has common part, eg:
 *  SELECT XX, YY, ZZ FROM TABLE_NAME
 * so we can prepare create common part, no need to create every query
 */
public class PrepareSqlQuery {

    private String commonFormatSql;

    private boolean containWhere;

    private int pageSize;

    private String orderBy;

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

    public String formatSql(Object... args) {
        return String.format(commonFormatSql, args);
    }

    @Override
    public String toString() {
        return "PrepareSqlQuery{" +
                "commonFormatSql='" + commonFormatSql + '\'' +
                ", orderBy='" + orderBy + '\'' +
                ", limit=" + pageSize +
                '}';
    }

    public static Build build() {
        return new Build();
    }

    public static class Build {

        private String commonFormatSql;

        private boolean containWhere;

        private int pageSize;

        public Build commonFormatSql(String commonFormatSql) {
            this.commonFormatSql = commonFormatSql;
            return this;
        }

        public Build containWhere() {
            this.containWhere = true;
            return this;
        }

        public Build pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public PrepareSqlQuery build() {
            PrepareSqlQuery prepareSqlQuery = new PrepareSqlQuery();
            prepareSqlQuery.commonFormatSql = commonFormatSql;
            prepareSqlQuery.containWhere = containWhere;
            prepareSqlQuery.pageSize = pageSize;
            return prepareSqlQuery;
        }
    }
}
