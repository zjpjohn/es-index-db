package com.wxingyl.es.jdal;

/**
 * Created by xing on 15/8/27.
 * db table field describe
 */
public class DbTableFieldDesc extends DbTableDesc {

    private String field;

    private DbTableDesc newTableDesc;

    public DbTableFieldDesc(String schema, String table, String field) {
        super(schema, table);
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public DbTableDesc newDbTableDesc() {
        if (newTableDesc == null) {
            newTableDesc = new DbTableDesc(getSchema(), getTable());
        }
        return newTableDesc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableFieldDesc)) return false;
        if (!super.equals(o)) return false;

        DbTableFieldDesc that = (DbTableFieldDesc) o;

        return field.equals(that.field);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DbTableFieldDesc{" +
                "schema='" + getSchema() + '\'' +
                "table='" + getTable() + '\'' +
                "field='" + field + '\'' +
                '}';
    }

    public static DbTableFieldDesc build(String schema, String table, String field) {
        return new DbTableFieldDesc(schema, table, field);
    }
}
