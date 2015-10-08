package com.wxingyl.es.util;

import com.wxingyl.es.db.DbTableDesc;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.collect.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/10/8.
 * DbTableDesc obj cache
 */
public final class TableDescCache {

    private static final TableDescCache INSTANCE = new TableDescCache();

    private Map<String, LoadingCache<String, DbTableDesc>> tableCache = new HashMap<>();

    private TableDescCache() {
    }

    private DbTableDesc get(final String schema, String table) {
        LoadingCache<String, DbTableDesc> cache = tableCache.get(schema);
        if (cache == null) {
            tableCache.put(schema, cache = CacheBuilder.newBuilder().weakValues()
                    .build(new CacheLoader<String, DbTableDesc>() {
                        @Override
                        public DbTableDesc load(String s) throws Exception {
                            return new DbTableDesc(schema, s);
                        }
                    }));
        }
        try {
            return cache.get(table);
        } catch (ExecutionException ignored) {
            return null;
        }
    }

    public static DbTableDesc getTableDesc(String schema, String table) {
        return INSTANCE.get(schema, table);
    }

    /**
     * only for use test
     * @return map
     */
    public static Map<String, Map<String, DbTableDesc>> asMap() {
        Map<String, Map<String, DbTableDesc>> ret = new HashMap<>(INSTANCE.tableCache.size());
        for (String k : INSTANCE.tableCache.keySet()) {
            ret.put(k, INSTANCE.tableCache.get(k).asMap());
        }
        return ret;
    }

}
