package com.wxingyl.es.conf.index;

import com.wxingyl.es.jdal.*;
import com.wxingyl.es.jdal.FilterMapListHandler;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xing on 15/8/24.
 * index type config bean, the result of parsing index_data.yml
 */
public class IndexTypeBean {

    private String index;

    private String type;

    private TableQuery masterTable;

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public TableQuery getMasterTable() {
        return masterTable;
    }

    public static class TableQuery {

        private SqlQueryHandle queryHandler;

        private String keyField;

        private String tableName;
        /**
         * filter {@link DbTableConfigInfo#forbidFields}
         * nullable
         */
        private FilterMapListHandler rsh;
        /**
         * key: field, value: salve table
         * unmodifiableMap
         */
        private Map<String, TableQuery> salveQuery;

        private PrepareSqlQuery commonSql;

        public SqlQueryHandle getQueryHandler() {
            return queryHandler;
        }

        public String getKeyField() {
            return keyField;
        }

        public String getTableName() {
            return tableName;
        }

        public FilterMapListHandler getRsh() {
            return rsh;
        }

        public Map<String, TableQuery> getSalveQuery() {
            return salveQuery;
        }

        public PrepareSqlQuery getCommonSql() {
            return commonSql;
        }
    }

    public static Build build(String index, String type) {
        return new Build(index, type);
    }

    public static class Build {

        private String index, type;

        private Map<DbTableDesc, Tuple<TableQuery, DbTableFieldDesc>> queryMap = new HashMap<>();

        public Build(String index, String type) {
            this.index = index;
            this.type = type;
        }

        public Build addTableQuery(SqlQueryHandle queryHandler, DbTableConfigInfo tableInfo) {
            TableQuery query = new TableQuery();
            query.queryHandler = queryHandler;
            query.tableName = tableInfo.getTableName();
            query.keyField = tableInfo.getRelationField();
            query.commonSql = query.queryHandler.createPrepareSqlQuery(tableInfo);
            if (!CommonUtils.isEmpty(tableInfo.getForbidFields())) {
                query.rsh = new FilterMapListHandler(tableInfo.getForbidFields());
            }
            queryMap.put(tableInfo.getTable(), Tuple.tuple(query, tableInfo.getMasterField()));
            return this;
        }

        public IndexTypeBean build(DbTableDesc masterTable) {
            IndexTypeBean bean = new IndexTypeBean();
            bean.index = index;
            bean.type = type;
            bean.masterTable = queryMap.get(masterTable).v1();
            Map<TableQuery, Map<String, TableQuery>> salveQueryBuild = new HashMap<>();
            queryMap.forEach((k, v) -> {
                DbTableFieldDesc masterField = v.v2();
                if (masterField == null) return;
                TableQuery master = queryMap.get(masterField.newDbTableDesc()).v1();
                Map<String, TableQuery> map = salveQueryBuild.get(master);
                if (map == null) {
                    salveQueryBuild.put(master, map = new HashMap<>());
                }
                map.put(masterField.getField(), v.v1());
            });
            salveQueryBuild.forEach((k, v) -> k.salveQuery = Collections.unmodifiableMap(v));
            return bean;
        }
    }
}
