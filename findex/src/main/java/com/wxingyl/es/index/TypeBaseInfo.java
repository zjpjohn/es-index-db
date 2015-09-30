package com.wxingyl.es.index;

import com.wxingyl.es.db.TableBaseInfo;

/**
 * Created by xing on 15/9/7.
 * type base info, such as index, type, table-name and so on
 */
public class TypeBaseInfo extends TableBaseInfo {

    private IndexTypeDesc type;

    public IndexTypeDesc getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeBaseInfo)) return false;
        if (!super.equals(o)) return false;

        TypeBaseInfo that = (TypeBaseInfo) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public static TypeBaseInfo build(TableBaseInfo orgObj, IndexTypeDesc type) {
        if (orgObj instanceof TypeBaseInfo) return (TypeBaseInfo) orgObj;
        TypeBaseInfo ret = new TypeBaseInfo();
        ret.table = orgObj.getTable();
        ret.keyField = orgObj.getKeyField();
        ret.masterAlias = orgObj.getMasterAlias();
        ret.mergeType = orgObj.getMergeType();
        ret.type = type;
        return ret;
    }
}
