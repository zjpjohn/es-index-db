package com.wxingyl.es.conf;

import com.wxingyl.es.util.CommonUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    LIST(list -> CommonUtils.isEmpty(list) ? null : list),
    /**
     * slave result as one, when slave result number more than one, we get list.get(0)
     */
    SINGLE(list -> CommonUtils.isEmpty(list) ? null : list.get(0)),
    /**
     * depend on slave result, only one result is single, multi result is list
     */
    AUTO(list -> {
        if (CommonUtils.isEmpty(list)) return null;
        else if (list.size() == 1) return list.get(0);
        else return list;
    });

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
