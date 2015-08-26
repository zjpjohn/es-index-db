package com.wxingyl.es.conf.index;

import com.wxingyl.es.conf.ds.DataSourceBean;
import com.wxingyl.es.jdal.FilterMapListHandler;
import com.wxingyl.es.util.CommonUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/24.
 * index type config bean, the result of parsing index_data.yml
 */
public class IndexTypeBean {

    private String index;

    private String type;

    private TableQuery tableQuery;

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public List<Map<String, Object>> dbQuery() throws SQLException {
        return tableQuery.query();
    }

    public static class TableQuery {

        private DataSourceBean dataSource;

        private String keyField;

        private String tableName;
        /**
         * filter {@link DbTableConfigInfo#forbidFields}
         */
        private FilterMapListHandler queryHandler;
        /**
         * key: field, value: salve table
         */
        private Map<String, TableQuery> salveQuery;

        private String querySql;

        List<Map<String, Object>> query() throws SQLException {
            return null;
        }
    }

    public static Build build(String index, String type) {
        return new Build(index, type);
    }

    public static class Build {

        private String index, type;

        private TableQuery dbQuery;

        public Build(String index, String type) {
            this.index = index;
            this.type = type;
        }

        public Build addMasterTable(DataSourceBean dataSource, DbTableConfigInfo masterTable) {
            dbQuery = createTableQuery(dataSource, masterTable);
            return this;
        }

        public Build addSalveQuery(DataSourceBean dataSource, DbTableConfigInfo tableInfo) {
            if (dbQuery.salveQuery == null) {
                dbQuery.salveQuery = new HashMap<>();
            }
            TableQuery salveQuery = createTableQuery(dataSource, tableInfo);
            dbQuery.salveQuery.put(tableInfo.getMasterField().v2(), salveQuery);
            return this;
        }

        private TableQuery createTableQuery(DataSourceBean dataSource, DbTableConfigInfo tableInfo) {
            TableQuery query = new TableQuery();
            query.dataSource = dataSource;
            query.tableName = tableInfo.getTableName();
            query.keyField = tableInfo.getRelationField();
            if (!CommonUtils.isEmpty(tableInfo.getForbidFields())) {
                query.queryHandler = new FilterMapListHandler(tableInfo.getForbidFields());
            }
            return query;
        }

        public IndexTypeBean build() {
            IndexTypeBean bean = new IndexTypeBean();
            bean.index = index;
            bean.type = type;
            bean.tableQuery = dbQuery;
            return bean;
        }
    }
}
