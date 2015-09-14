package com.wxingyl.es.db.query;

import org.elasticsearch.common.collect.Tuple;

import java.util.Set;

/**
 * Created by xing on 15/9/11.
 * sql query statement structure
 */
public interface SqlQueryStatementStructure {

    StringBuilder appendField(StringBuilder sb, String field);

    default StringBuilder appendValue(StringBuilder sb, String value) {
        return sb.append('\'').append(value).append('\'');
    }

    StringBuilder singleValueAppend(StringBuilder sb, QueryCondition<String> condition);

    StringBuilder rangeValueAppend(StringBuilder sb, QueryCondition<Tuple<String, String>> condition);

    StringBuilder listValueAppend(StringBuilder sb, QueryCondition<Set<String>> condition);
}
