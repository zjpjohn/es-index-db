package com.wxingyl.es.jdal.handle;

import com.wxingyl.es.jdal.DbTableDesc;
import org.apache.commons.dbutils.QueryRunner;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.collect.Tuple;

import javax.sql.DataSource;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/8/26.
 * abstract sql query handler
 */
public abstract class AbstractSqlQueryHandler implements SqlQueryHandle {

    protected QueryRunner queryRunner;

    private LoadingCache<String, Set<String>> schemaTablesCache;

    private LoadingCache<DbTableDesc, Set<String>> tableFieldsCache;

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
                .build(new CacheLoader<DbTableDesc, Set<String>>() {
                    @Override
                    public Set<String> load(DbTableDesc table) throws Exception {
                        return loadAllFields(table);
                    }
                });
    }

    protected abstract Set<String> loadAllTables(String schema) throws Exception;

    protected abstract Set<String> loadAllFields(DbTableDesc table) throws Exception;

    @Override
    public Set<String> getAllTables(String schema) throws ExecutionException {
        return schemaTablesCache.get(schema);
    }

    @Override
    public Set<String> getAllFields(DbTableDesc table) throws ExecutionException {
        return tableFieldsCache.get(table);
    }
}
