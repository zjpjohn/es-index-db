package com.wxingyl.es.jdal;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.collect.Tuple;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * abstract sql query handler
 */
public abstract class AbstractSqlQueryHandler implements SqlQueryHandle {

    protected static final MapListHandler DEFAULT_MAP_LIST_HANDLER = new MapListHandler();

    protected QueryRunner queryRunner;

    private LoadingCache<String, Set<String>> schemaTablesCache;

    private LoadingCache<Tuple<String, String>, Set<String>> tableFieldsCache;

    public AbstractSqlQueryHandler(DataSource dataSource) {
        queryRunner = new QueryRunner(dataSource);
        schemaTablesCache = CacheBuilder.newBuilder()
                .weakKeys()
                .weakValues()
                .build(new CacheLoader<String, Set<String>>() {
                    @Override
                    public Set<String> load(String schema) throws Exception {
                        return loadAllTables(schema);
                    }
                });
        tableFieldsCache = CacheBuilder.newBuilder()
                .weakKeys()
                .weakValues()
                .build(new CacheLoader<Tuple<String, String>, Set<String>>() {
                    @Override
                    public Set<String> load(Tuple<String, String> tuple) throws Exception {
                        return loadAllFields(tuple.v1(), tuple.v2());
                    }
                });
    }

    @Override
    public List<Map<String, Object>> query(String sql, ResultSetHandler<List<Map<String, Object>>> handler) throws SQLException {
        return queryRunner.query(sql, handler == null ? DEFAULT_MAP_LIST_HANDLER : handler);
    }

    protected abstract Set<String> loadAllTables(String schema) throws Exception;

    protected abstract Set<String> loadAllFields(String schema, String table) throws Exception;

    @Override
    public Set<String> getAllTables(String schema) throws Exception {
        return schemaTablesCache.get(schema);
    }

    @Override
    public Set<String> getAllFields(String schema, String table) throws Exception {
        return tableFieldsCache.get(Tuple.tuple(schema, table));
    }
}
