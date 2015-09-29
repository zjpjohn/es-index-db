package com.wxingyl.es.db.query;

import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Tuple;

import java.util.Set;

/**
 * Created by xing on 15/9/11.
 * sql query condition
 */
public abstract class QueryCondition<T> {

    private String field;

    private SqlQueryOperator op;

    protected T value;

    public QueryCondition(String field, SqlQueryOperator op) {
        this.field = field;
        this.op = op;
    }

    public String getField() {
        return field;
    }

    public SqlQueryOperator getOp() {
        return op;
    }

    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryCondition)) return false;

        QueryCondition<?> that = (QueryCondition<?>) o;

        return field.equals(that.field);

    }

    @Override
    public int hashCode() {
        return field.hashCode();
    }

    public abstract StringBuilder appendQuerySql(StringBuilder sb, SqlQueryStatementStructure structure);

    public static QueryCondition build(String queryStr) {
        String[] queries = CommonUtils.split(queryStr, ':');
        if (queries.length < 3) return null;
        SqlQueryOperator op;
        if (queries[1].isEmpty()) {
            op = SqlQueryOperator.EQ;
        } else {
            op = SqlQueryOperator.getOp(queries[1]);
        }
        if (op == null) return null;
        if (op == SqlQueryOperator.RANGE) {
            return buildRange(queries[0], op, Tuple.tuple(queries[2], queries[3]));
        } else if (op == SqlQueryOperator.IN || op == SqlQueryOperator.NIN) {
            return buildList(queries[0], op, ImmutableSet.copyOf(CommonUtils.split(queries[2])));
        } else {
            return buildSingle(queries[0], op, queries[2]);
        }
    }

    public static SingleQueryCondition buildSingle(String field, SqlQueryOperator op, String value) {
        SingleQueryCondition ret = new SingleQueryCondition(field, op);
        ret.value = value;
        return ret;
    }

    public static RangeQueryCondition buildRange(String field, SqlQueryOperator op, Tuple<String, String> value) {
        RangeQueryCondition ret = new RangeQueryCondition(field, op);
        ret.value = value;
        return ret;
    }

    public static ListQueryCondition buildList(String field, SqlQueryOperator op, Set<String> set) {
        ListQueryCondition ret = new ListQueryCondition(field, op);
        ret.value = set;
        return ret;
    }

    public static class SingleQueryCondition extends QueryCondition<String> {

        public SingleQueryCondition(String field, SqlQueryOperator op) {
            super(field, op);
        }

        @Override
        public StringBuilder appendQuerySql(StringBuilder sb, SqlQueryStatementStructure structure) {
            return structure.singleValueAppend(sb, this);
        }

    }

    public static class RangeQueryCondition extends QueryCondition<Tuple<String, String>> {

        public RangeQueryCondition(String field, SqlQueryOperator op) {
            super(field, op);
        }

        @Override
        public StringBuilder appendQuerySql(StringBuilder sb, SqlQueryStatementStructure structure) {
            return structure.rangeValueAppend(sb, this);
        }

    }

    public static class ListQueryCondition extends QueryCondition<Set<String>> {

        public ListQueryCondition(String field, SqlQueryOperator op) {
            super(field, op);
        }

        @Override
        public StringBuilder appendQuerySql(StringBuilder sb, SqlQueryStatementStructure structure) {
            return structure.listValueAppend(sb, this);
        }

    }

}
