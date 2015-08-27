package com.wxingyl.es.conf.index;

import com.wxingyl.es.jdal.*;
import com.wxingyl.es.jdal.handle.FilterMapListHandler;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Tuple;

import java.util.HashMap;
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

    public static class TableQuery {

        private SqlQueryHandle queryHandler;

        private String keyField;

        private String tableName;
        /**
         * filter {@link DbTableConfigInfo#forbidFields}
         */
        private FilterMapListHandler rsh;
        /**
         * key: field, value: salve table
         */
        private Map<String, TableQuery> salveQuery;

        private PrepareSqlQuery commonSql;
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
            bean.tableQuery = queryMap.get(masterTable).v1();
            //TODO deal depend
            return bean;
        }
    }
}
