package com.wxingyl.es.db;

/**
 * Created by xing on 15/8/27.
 * db table field describe
 */
public class DbTableFieldDesc {

    private String field;

    private DbTableDesc table;

    public DbTableFieldDesc(DbTableDesc tableDesc, String field) {
        this.table = tableDesc;
        this.field = field.toLowerCase();
    }

    public String getField() {
        return field;
    }

    public DbTableDesc getTableDesc() {
        return table;
    }

    public String getSchema() {
        return table.getSchema();
    }

    public String getTable() {
        return table.getTable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableFieldDesc)) return false;

        DbTableFieldDesc that = (DbTableFieldDesc) o;

        if (!field.equals(that.field)) return false;
        return table.equals(that.table);

    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + table.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return '[' + getSchema() + '.' + getTable() + '.' + field + ']';
    }
}
