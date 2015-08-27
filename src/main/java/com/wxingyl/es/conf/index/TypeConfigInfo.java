package com.wxingyl.es.conf.index;

import com.wxingyl.es.jdal.DbTableDesc;

import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * parse config file, get type info
 */
public class TypeConfigInfo {

    private String index;

    private String type;

    private DbTableDesc masterTable;

    private Set<DbTableConfigInfo> tables;

    public void setIndexType(String index, String type) {
        this.index = index;
        this.type = type;
    }

    public void setMasterTable(DbTableDesc masterTable) {
        this.masterTable = masterTable;
    }

    public void setTables(Set<DbTableConfigInfo> tables) {
        this.tables = tables;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public DbTableDesc getMasterTable() {
        return masterTable;
    }

    public Set<DbTableConfigInfo> getTables() {
        return tables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeConfigInfo)) return false;

        TypeConfigInfo type = (TypeConfigInfo) o;

        if (!index.equals(type.index)) return false;
        return this.type.equals(type.type);
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "[index: " + index + ", type: " + type + ", masterTable: " + masterTable + "]";
    }
}
