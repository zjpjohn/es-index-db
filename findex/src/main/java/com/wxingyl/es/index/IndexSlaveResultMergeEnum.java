package com.wxingyl.es.index;

import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.common.base.Function;

import java.util.List;
import java.util.Map;

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
    LIST(new Function<List<Map<String, Object>>, Object>() {
        @Override
        public Object apply(List<Map<String, Object>> list) {
            return CommonUtils.isEmpty(list) ? null : list;
        }
    }),
    /**
     * slave result as one, when slave result number more than one, we get list.get(0)
     */
    SINGLE(new Function<List<Map<String, Object>>, Object>() {
        @Override
        public Object apply(List<Map<String, Object>> list) {
            return CommonUtils.isEmpty(list) ? null : list.get(0);
        }
    }),
    /**
     * depend on slave result, only one result is single, multi result is list
     */
    AUTO(new Function<List<Map<String, Object>>, Object>() {
        @Override
        public Object apply(List<Map<String, Object>> list) {
            if (CommonUtils.isEmpty(list)) return null;
            else if (list.size() == 1) return list.get(0);
            else return list;
        }
    }),
    /**
     * slave result as one, and auto merge slave result to master table
     * Note: There is same name when merging, will add "{@link TableBaseInfo#masterAlias} _" before field name
     */
    MERGE(SINGLE.function);

    private Function<List<Map<String, Object>>, Object> function;

    IndexSlaveResultMergeEnum(Function<List<Map<String, Object>>, Object> function) {
        this.function = function;
    }

    public Object function(List<Map<String, Object>> list) {
        return function.apply(list);
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
