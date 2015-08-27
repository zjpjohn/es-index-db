package com.wxingyl.es.jdal;

/**
 * Created by xing on 15/8/27.
 * db table describe
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

    public static DbTableDesc build(String schema, String table) {
        return new DbTableDesc(schema, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableDesc)) return false;

        DbTableDesc that = (DbTableDesc) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        return table.equals(that.table);

    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + table.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DbTableDesc{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
