package com.wxingyl.es.conf.index;

import com.wxingyl.es.jdal.*;
import com.wxingyl.es.jdal.FilterMapListHandler;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbutils.ResultSetHandler;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

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

        /**
         * filter {@link DbTableConfigInfo#forbidFields}
         * nullable
         */
        private FilterMapListHandler rsh;
        /**
         * key: salve table, value: field
         * unmodifiableMap
         */
        private Map<TableQuery, String> slaveQuery;

        private PrepareSqlQuery commonSql;

        public SqlQueryHandle getQueryHandler() {
            return queryHandler;
        }

        public FilterMapListHandler getRsh() {
            return rsh;
        }

        public Map<TableQuery, String> getSlaveQuery() {
            return slaveQuery;
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

        private Map<DbTableDesc, Tuple<TableQuery, DbTableFieldDesc>> tableMap = new HashMap<>();

        public Build(String index, String type) {
            this.index = index;
            this.type = type;
        }

        public Build addTableQuery(SqlQueryHandle queryHandler, DbTableConfigInfo tableInfo) {
            TableQuery query = new TableQuery();
            query.queryHandler = queryHandler;
            query.commonSql = query.queryHandler.createPrepareSqlQuery(tableInfo);
            if (CommonUtils.isEmpty(tableInfo.getForbidFields())) {
                query.rsh = SqlQueryHandle.DEFAULT_MAP_LIST_HANDLER;
            } else {
                query.rsh = new FilterMapListHandler(tableInfo.getForbidFields());
            }
            tableMap.put(tableInfo.getTable(), Tuple.tuple(query, tableInfo.getMasterField()));
            return this;
        }

        public IndexTypeBean build(DbTableDesc masterTable,
                                   BiConsumer<Tuple<DbTableDesc, SqlQueryHandle>, List<String>> masterAliasVerify) {
            IndexTypeBean bean = new IndexTypeBean();
            bean.index = index;
            bean.type = type;
            bean.masterTable = tableMap.get(masterTable).v1();
            // key: master table, value: v1.key: salve table v1.value: master field, v2: master alias list
            Map<TableQuery, Tuple<Map<TableQuery, String>, List<String>>> salveQueryBuild = new HashMap<>();
            Map<TableQuery, DbTableDesc> queryTableMap = new HashMap<>();
            tableMap.forEach((table, v) -> {
                DbTableFieldDesc masterField = v.v2();
                if (masterField == null) return;
                TableQuery masterQuery = tableMap.get(masterField.newDbTableDesc()).v1();
                Tuple<Map<TableQuery, String>, List<String>> tuple = salveQueryBuild.get(masterQuery);
                if (tuple == null) {
                    salveQueryBuild.put(masterQuery, tuple = Tuple.tuple(new HashMap<>(), new ArrayList<>()));
                    queryTableMap.put(masterQuery, masterField.newDbTableDesc());
                }
                TableQuery salveQuery = v.v1();
                tuple.v1().put(salveQuery, masterField.getField());
                tuple.v2().add(salveQuery.getCommonSql().getMasterAlias());
            });

            salveQueryBuild.forEach((masterQuery, tuple) -> {
                masterQuery.slaveQuery = Collections.unmodifiableMap(tuple.v1());
                masterAliasVerify.accept(Tuple.tuple(queryTableMap.get(masterQuery), masterQuery.getQueryHandler()), tuple.v2());
            });
            return bean;
        }
    }
}
