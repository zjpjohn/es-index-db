package com.wxingyl.es.db.query;

/**
 * Created by xing on 15/9/11.
 * sql query operator
 */
public enum SqlQueryOperator {
    EQ("="),
    NE("!="),
    GT(">"),
    GE(">="),
    LT("<"),
    LE("<="),
    IN("in"),
    NIN("nin"),
    RANGE("range");

    private String op;

    SqlQueryOperator(String op) {
        this.op = op;
    }

    public static SqlQueryOperator getOp(String op) {
        for (SqlQueryOperator e : values()) {
            if (e.op.equals(op)) return e;
        }
        return null;
    }

    @Override
    public String toString() {
        return op;
    }
}
