package com.wxingyl.es.conf.index;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.jdal.*;
import com.wxingyl.es.jdal.FilterMapListHandler;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Created by xing on 15/8/24.
 * index type config bean, the result of parsing index_data.yml
 */
public class IndexTypeBean {

    private IndexTypeDesc type;

    private TableQueryInfo masterTable;

    private IndexTypeBean() {}

    public IndexTypeDesc getType() {
        return type;
    }

    public TableQueryInfo getMasterTable() {
        return masterTable;
    }

    public static Builder build(IndexTypeDesc type) {
        return new Builder(type);
    }

    public static class Builder {

        private IndexTypeDesc type;

        private Map<DbTableDesc, Tuple<TableQueryInfo.Builder, DbTableFieldDesc>> tableMap = new HashMap<>();

        public Builder(IndexTypeDesc type) {
            this.type = type;
        }

        public Builder addTableQuery(SqlQueryHandle queryHandler, DbTableConfigInfo tableInfo) {
            TableQueryInfo.Builder queryBuilder = TableQueryInfo.build();
            queryBuilder.queryHandler(queryHandler)
                    .queryCommon(queryHandler.createPrepareSqlQuery(tableInfo))
                    .rsh(CommonUtils.isEmpty(tableInfo.getForbidFields()) ? SqlQueryHandle.DEFAULT_MAP_LIST_HANDLER
                            : new FilterMapListHandler(tableInfo.getForbidFields()));
            tableMap.put(tableInfo.getTable(), Tuple.tuple(queryBuilder, tableInfo.getMasterField()));
            return this;
        }

        public IndexTypeBean build(DbTableDesc masterTable,
                                   BiConsumer<TableQueryInfo, List<String>> masterAliasVerify) {
            IndexTypeBean bean = new IndexTypeBean();
            bean.type = type;
            tableMap.values().forEach(v -> {
                v.v1().masterAliasVerify(masterAliasVerify);
                DbTableFieldDesc field = v.v2();
                if (field == null) return;
                TableQueryInfo.Builder builder = tableMap.get(field.newDbTableDesc()).v1();
                builder.addSlave(v.v1(), field.getField());
            });
            bean.masterTable = tableMap.get(masterTable).v1().build();
            return bean;
        }
    }
}
