package com.wxingyl.es.db;

/**
 * Created by xing on 15/8/27.
 * db table describe
 * the urlAddress can null, so when DbTableDesc as key, you should carefully
 */
public class DbTableDesc {

    private String schema;

    private String table;

    private String urlAddress;

    public DbTableDesc(String urlAddress, String schema, String table) {
        this.schema = schema;
        this.table = table;
        this.urlAddress = urlAddress;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getUrlAddress() {
        return urlAddress;
    }

    public boolean equalsIgnoreUrlAddress(DbTableDesc tableDesc) {
        if (this == tableDesc) return true;
        if (!schema.equals(tableDesc.schema)) return false;
        return table.equals(tableDesc.table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DbTableDesc)) return false;

        DbTableDesc tableDesc = (DbTableDesc) o;

        if (!schema.equals(tableDesc.schema)) return false;
        if (!table.equals(tableDesc.table)) return false;
        return !(urlAddress != null ? !urlAddress.equals(tableDesc.urlAddress) : tableDesc.urlAddress != null);
    }

    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + table.hashCode();
        result = 31 * result + (urlAddress != null ? urlAddress.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DbTableDesc{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", urlAddress='" + urlAddress + '\'' +
                '}';
    }
}
