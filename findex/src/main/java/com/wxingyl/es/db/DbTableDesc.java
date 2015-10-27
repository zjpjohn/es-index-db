package com.wxingyl.es.db;

/**
 * Created by xing on 15/8/27.
 * db table describe
 * the urlAddress can null, so when DbTableDesc as key, you should carefully
 */
public class DbTableDesc {

    private String schema;

    private String table;

    public DbTableDesc(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableDesc)) return false;

        DbTableDesc tableDesc = (DbTableDesc) o;

        if (!schema.equals(tableDesc.schema)) return false;
        return table.equals(tableDesc.table);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + table.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return '[' + schema + '.' + table + ']';
    }
}
