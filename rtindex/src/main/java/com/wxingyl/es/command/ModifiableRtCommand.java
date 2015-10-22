package com.wxingyl.es.command;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by xing on 15/10/23.
 * modifiable real time command
 */
public interface ModifiableRtCommand extends RtCommand {
    /**
     * query which doc to modify
     */
    void addPreQuery(QueryBuilder queryBuilder);

    void addPreFilter(FilterBuilder filterBuilder);
}
