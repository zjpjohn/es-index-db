package com.wxingyl.es.conf.index;

import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/26.
 * parse config file, get type info
 */
public class TypeConfigInfo {

    String index;

    String type;

    String masterTable;
    /**
     * key: table name, value: table info
     */
    Set<DbTableConfigInfo> tables;
    //key: master table, value: slave table set
    Map<String, Set<String>> dependedTable;

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public String getMasterTable() {
        return masterTable;
    }

    public Set<DbTableConfigInfo> getTables() {
        return tables;
    }

    public Map<String, Set<String>> getDependedTable() {
        return dependedTable;
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
