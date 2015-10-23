package com.wxingyl.es.command;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by xing on 15/10/23.
 * need pre query, get document, and then make some replace
 */
public interface PreQueryRtCommand extends RtCommand {
    /**
     * query which doc to modify
     */
    void addPreQuery(QueryBuilder queryBuilder);

    void addPreFilter(FilterBuilder filterBuilder);

    /**
     * have query, we must need pageSize
     */
    boolean needContinue();
}
