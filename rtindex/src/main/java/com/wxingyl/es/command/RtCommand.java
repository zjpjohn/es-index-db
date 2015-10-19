package com.wxingyl.es.command;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by xing on 15/10/15.
 * real-time index update/insert/delete command
 */
public interface RtCommand {
    /**
     * query which doc to modify
     */
    void addPreQuery(QueryBuilder queryBuilder);

    void addPreFilter(FilterBuilder filterBuilder);

    boolean isInvalid();

}
