package com.wxingyl.es.util;

import com.wxingyl.es.index.IndexTypeDesc;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/11/2.
 * {@link IndexTypeDesc} obj cache
 */
public final class TypeDescCache {

    private static final TypeDescCache INSTANCE = new TypeDescCache();

    private Map<String, LoadingCache<String, IndexTypeDesc>> typeCache = new HashMap<>();

    private TypeDescCache() {
    }

    private IndexTypeDesc get(final String index, final String type) {
        LoadingCache<String, IndexTypeDesc> cache = typeCache.get(index);
        if (cache == null) {
            typeCache.put(index, cache = CacheBuilder.newBuilder().weakValues()
                    .build(new CacheLoader<String, IndexTypeDesc>() {
                        @Override
                        public IndexTypeDesc load(String s) throws Exception {
                            return new IndexTypeDesc(index, s);
                        }
                    }));
        }
        try {
            return cache.get(type);
        } catch (ExecutionException ignored) {
            return null;
        }
    }

    public static IndexTypeDesc getTypeDesc(String index, String type) {
        return INSTANCE.get(index, type);
    }

    /**
     * only for use test
     * @return map
     */
    public static Map<String, Map<String, IndexTypeDesc>> asMap() {
        Map<String, Map<String, IndexTypeDesc>> ret = new HashMap<>(INSTANCE.typeCache.size());
        for (String k : INSTANCE.typeCache.keySet()) {
            ret.put(k, INSTANCE.typeCache.get(k).asMap());
        }
        return ret;
    }

}
