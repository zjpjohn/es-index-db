package com.wxingyl.es.conf.index;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.dbquery.DbTableDesc;

import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * parse config file, get type info
 */
public class TypeConfigInfo {

    private IndexTypeDesc typeDesc;

    private DbTableDesc masterTable;

    private Set<DbTableConfigInfo> tables;

    public TypeConfigInfo(IndexTypeDesc typeDesc) {
        this.typeDesc = typeDesc;
    }

    public void setMasterTable(DbTableDesc masterTable) {
        this.masterTable = masterTable;
    }

    public void setTables(Set<DbTableConfigInfo> tables) {
        this.tables = tables;
    }

    public IndexTypeDesc getTypeDesc() {
        return typeDesc;
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

        TypeConfigInfo that = (TypeConfigInfo) o;

        return typeDesc.equals(that.typeDesc);

    }

    @Override
    public int hashCode() {
        return typeDesc.hashCode();
    }

    @Override
    public String toString() {
        return "[type: " + typeDesc + ", masterTable: " + masterTable + "]";
    }
}
