package com.wxingyl.es.db.query;

import org.elasticsearch.common.collect.Tuple;

import java.util.Set;

/**
 * Created by xing on 15/9/11.
 * default mysql query statement structure
 */
public class MysqlQueryStatementStructure implements SqlQueryStatementStructure {

    @Override
    public StringBuilder appendField(StringBuilder sb, String field) {
        return sb.append('`').append(field).append('`').append(' ');
    }

    @Override
    public StringBuilder singleValueAppend(StringBuilder sb, QueryCondition<String> condition) {
        return appendValue(appendField(sb, condition.getField()).append(condition.getOp()).append(' '), condition.getValue());
    }

    @Override
    public StringBuilder rangeValueAppend(StringBuilder sb, QueryCondition<Tuple<String, String>> condition) {
        sb = appendValue(appendField(sb, condition.getField()).append("BETWEEN "), condition.getValue().v1());
        return appendValue(sb.append(" VALUE "), condition.getValue().v2());
    }

    @Override
    public StringBuilder listValueAppend(StringBuilder sb, QueryCondition<Set<String>> condition) {
        appendField(sb, condition.getField());
        if (condition.getOp() == SqlQueryOperator.NIN) sb.append(" NOT");
        sb.append(" IN (");
        condition.getValue().forEach(v -> {
            appendValue(sb, v).append(", ");
        });
        sb.delete(sb.length()-2, sb.length());
        sb.append(")");
        return sb;
    }
}
