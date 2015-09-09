package com.wxingyl.es.conf.index;

import com.wxingyl.es.dbquery.SqlQueryCommon;
import com.wxingyl.es.dbquery.SqlQueryHandle;
import com.wxingyl.es.dbquery.TableQueryBaseInfo;
import org.apache.commons.dbutils.ResultSetHandler;
import org.elasticsearch.common.collect.ImmutableListMultimap;
import org.elasticsearch.common.collect.ImmutableMultimap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Created by xing on 15/9/1.
 * field of indexTypeBean, table query info
 */
public class TableQueryInfo {

    private SqlQueryHandle queryHandler;

    /**
     * filter {@link DbTableConfigInfo#forbidFields}
     * nullable
     */
    private ResultSetHandler<List<Map<String, Object>>> rsh;

    private SqlQueryCommon queryCommon;

    /**
     * key: salve table, value: field
     * unmodifiableMap
     */
    private ImmutableMultimap<String, TableQueryInfo> slaveQuery;

    private TableQueryInfo() {}

    public SqlQueryHandle getQueryHandler() {
        return queryHandler;
    }

    public ResultSetHandler<List<Map<String, Object>>> getRsh() {
        return rsh;
    }

    /**
     * @return unmodifiableMap
     */
    public ImmutableMultimap<String, TableQueryInfo> getSlaveQuery() {
        return slaveQuery;
    }

    public SqlQueryCommon getQueryCommon() {
        return queryCommon;
    }

    public void allTableQueryBaseInfo(List<TableQueryBaseInfo> list) {
        if (list == null) return;
        list.add(queryCommon.getTableQueryBaseInfo());
        if (slaveQuery != null) {
            slaveQuery.values().forEach(v -> v.allTableQueryBaseInfo(list));
        }
    }

    static Builder build() {
        return new Builder();
    }

    public static class Builder {

        private SqlQueryHandle queryHandler;

        private ResultSetHandler<List<Map<String, Object>>> rsh;

        private SqlQueryCommon queryCommon;

        private Map<Builder, String> slaveMap = new HashMap<>();

        private BiConsumer<TableQueryInfo, List<String>> masterAliasVerify;

        private TableQueryInfo obj;

        public Builder queryHandler(SqlQueryHandle handle) {
            this.queryHandler = handle;
            return this;
        }

        public Builder rsh(ResultSetHandler<List<Map<String, Object>>> rsh) {
            this.rsh = rsh;
            return this;
        }

        public Builder queryCommon(SqlQueryCommon queryCommon) {
            this.queryCommon = queryCommon;
            return this;
        }

        public Builder addSlave(Builder slave, String field) {
            if (slave != this) {
                slaveMap.put(slave, field);
            }
            return this;
        }

        public Builder masterAliasVerify(BiConsumer<TableQueryInfo, List<String>> verify) {
            masterAliasVerify = verify;
            return this;
        }

        public TableQueryInfo build() {
            if (obj != null) return obj;
            obj = new TableQueryInfo();
            obj.queryCommon = queryCommon;
            obj.queryHandler = queryHandler;
            obj.rsh = rsh;
            if (!slaveMap.isEmpty()) {
                final List<String> masterAlias = new ArrayList<>(slaveMap.size());
                final ImmutableListMultimap.Builder<String, TableQueryInfo> mapBuilder = ImmutableListMultimap.builder();
                slaveMap.forEach((k, v) -> {
                    mapBuilder.put(v, k.build());
                    masterAlias.add(k.queryCommon.getMasterAlias());
                });
                obj.slaveQuery = mapBuilder.build();
                masterAliasVerify.accept(obj, masterAlias);
            }
            return obj;
        }
    }
}
