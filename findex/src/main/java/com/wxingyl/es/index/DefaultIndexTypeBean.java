package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.db.query.TableQueryInfo;
import com.wxingyl.es.db.*;
import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.util.BiConsumer;
import org.apache.commons.dbutils.ResultSetHandler;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;

/**
 * Created by xing on 15/8/24.
 * index type config bean, the result of parsing index_data.yml
 */
public class DefaultIndexTypeBean implements IndexTypeBean {

    private IndexTypeDesc type;

    private TableQueryInfo masterTable;

    private Map<DbTableDesc, SqlQueryCommon> allTableQueryInfo;

    private int priority;

    @Override
    public IndexTypeDesc getType() {
        return type;
    }

    @Override
    public TableQueryInfo getMasterTable() {
        return masterTable;
    }

    @Override
    public SqlQueryCommon getTableQueryInfo(DbTableDesc table) {
        return allTableQueryInfo.get(table);
    }

    /**
     * @return unmodifiable list
     */
    @Override
    public List<SqlQueryCommon> getAllTableQueryInfo() {
        if (allTableQueryInfo == null) {
            List<SqlQueryCommon> list = new ArrayList<>();
            masterTable.allSqlQueryCommon(list);
            allTableQueryInfo = new HashMap<>();
            for (SqlQueryCommon common : list) {
                allTableQueryInfo.put(common.getTable(), common);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(allTableQueryInfo.values()));
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DefaultIndexTypeBean)) return false;

        DefaultIndexTypeBean bean = (DefaultIndexTypeBean) o;

        return type.equals(bean.type);

    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public int compareTo(IndexTypeBean o) {
        return o.getPriority() - priority;
    }

    public static class Builder {

        private IndexTypeDesc type;

        private Map<DbTableDesc, Tuple<TableQueryInfo.Builder, DbTableFieldDesc>> tableMap = new HashMap<>();

        private int priority;

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder type(IndexTypeDesc type) {
            this.type = type;
            return this;
        }

        public Builder addTableQuery(SqlQueryHandle queryHandler, DbTableConfigInfo tableInfo,
                                     ResultSetHandler<List<Map<String, Object>>> rsh) {
            TableQueryInfo.Builder queryBuilder = TableQueryInfo.build();
            queryBuilder.queryHandler(queryHandler)
                    .queryCommon(queryHandler.createPrepareSqlQuery(tableInfo))
                    .rsh(rsh);
            tableMap.put(tableInfo.getTable(), Tuple.tuple(queryBuilder, tableInfo.getMasterField()));
            return this;
        }

        public IndexTypeBean build(DbTableDesc masterTable,
                                   BiConsumer<TableQueryInfo, List<String>> masterAliasVerify) {
            DefaultIndexTypeBean bean = new DefaultIndexTypeBean();
            bean.type = type;
            for (Tuple<TableQueryInfo.Builder, DbTableFieldDesc> v : tableMap.values()) {
                v.v1().masterAliasVerify(masterAliasVerify);
                DbTableFieldDesc field = v.v2();
                if (field == null) continue;
                TableQueryInfo.Builder builder = tableMap.get(field.getTableDesc()).v1();
                builder.addSlave(v.v1(), field.getField());
            }
            bean.masterTable = tableMap.get(masterTable).v1().build();
            tableMap.clear();
            bean.priority = priority;
            return bean;
        }
    }
}
