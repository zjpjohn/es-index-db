package com.wxingyl.es.jdal;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/8/30.
 * query db result
 */
public class DbQueryResult {

    private int paramSize;

    private List<Map<String, Object>> dbData;

    public int getParamSize() {
        return paramSize;
    }

    public boolean needContinue() {
        return dbData.size() == paramSize;
    }
}
