package com.wxingyl.es.conf.index;

import com.wxingyl.es.jdal.FilterMapListHandler;
import com.wxingyl.es.jdal.SqlQueryCommon;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;

import java.util.*;
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
    private FilterMapListHandler rsh;

    private SqlQueryCommon queryCommon;

    /**
     * key: salve table, value: field
     * unmodifiableMap
     */
    private Map<TableQueryInfo, String> slaveQuery;

    public SqlQueryHandle getQueryHandler() {
        return queryHandler;
    }

    public FilterMapListHandler getRsh() {
        return rsh;
    }

    /**
     * @return unmodifiableMap
     */
    public Map<TableQueryInfo, String> getSlaveQuery() {
        return slaveQuery;
    }

    public SqlQueryCommon getQueryCommon() {
        return queryCommon;
    }

    static Builder build() {
        return new Builder();
    }

    static class Builder {

        private SqlQueryHandle queryHandler;

        private FilterMapListHandler rsh;

        private SqlQueryCommon queryCommon;

        private Map<Builder, String> slaveMap = new HashMap<>();

        private BiConsumer<TableQueryInfo, List<String>> masterAliasVerify;

        private TableQueryInfo obj;

        Builder queryHandler(SqlQueryHandle handle) {
            this.queryHandler = handle;
            return this;
        }

        Builder rsh(FilterMapListHandler rsh) {
            this.rsh = rsh;
            return this;
        }

        Builder queryCommon(SqlQueryCommon queryCommon) {
            this.queryCommon = queryCommon;
            return this;
        }

        Builder addSlave(Builder slave, String field) {
            if (slave != this) {
                slaveMap.put(slave, field);
            }
            return this;
        }

        Builder masterAliasVerify(BiConsumer<TableQueryInfo, List<String>> verify) {
            masterAliasVerify = verify;
            return this;
        }

        TableQueryInfo build() {
            if (obj != null) return obj;
            obj = new TableQueryInfo();
            obj.queryCommon = queryCommon;
            obj.queryHandler = queryHandler;
            obj.rsh = rsh;
            if (!slaveMap.isEmpty()) {
                final List<String> masterAlias = new ArrayList<>(slaveMap.size());
                final Map<TableQueryInfo, String> map = new HashMap<>();
                slaveMap.forEach((k, v) -> {
                    map.put(k.build(), v);
                    masterAlias.add(k.queryCommon.getMasterAlias());
                });
                obj.slaveQuery = Collections.unmodifiableMap(map);
                masterAliasVerify.accept(obj, masterAlias);
            }
            return obj;
        }
    }
}
