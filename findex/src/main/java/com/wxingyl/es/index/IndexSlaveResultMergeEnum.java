package com.wxingyl.es.index;

import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Function;

import java.util.List;

/**
 * Created by xing on 15/9/7.
 * query slave table, merge it to master, if a master value there are relation value in slave more then one,
 * we should have principle to handle it
 */
public enum IndexSlaveResultMergeEnum {
    /**
     * slave result as list, no matter what slave result is only one
     * Note: it is default
     */
    LIST(new Function<List, Object>() {
        @Override
        public Object apply(List list) {
            return CommonUtils.isEmpty(list) ? null : list;
        }
    }),
    /**
     * slave result as one, when slave result number more than one, we get list.get(0)
     */
    SINGLE(new Function<List, Object>() {
        @Override
        public Object apply(List list) {
            return CommonUtils.isEmpty(list) ? null : list.get(0);
        }
    }),
    /**
     * depend on slave result, only one result is single, multi result is list
     */
    AUTO(new Function<List, Object>() {
        @Override
        public Object apply(List list) {
            if (CommonUtils.isEmpty(list)) return null;
            else if (list.size() == 1) return list.get(0);
            else return list;
        }
    }),
    /**
     * slave result as one, and auto merge slave result to master table
     * Note: There is same name when merging, will add "{@link TableBaseInfo#masterAlias} _" before field name
     */
    MERGE(AUTO.function);

    private Function<List, Object> function;

    IndexSlaveResultMergeEnum(Function<List, Object> function) {
        this.function = function;
    }

    public Object function(List list) {
        return function.apply(list);
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
