package com.wxingyl.es.conf.index;

import com.wxingyl.es.conf.ConfigParse;
import com.wxingyl.es.util.CommonUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/9/8.
 * index config parse
 */
public interface IndexConfigParse extends ConfigParse<TypeConfigInfo> {

    default Set<TypeConfigInfo> parseAll(Map<String, Map<String, Object>> configMap) {
        Set<TypeConfigInfo> set = new HashSet<>();
        configMap.forEach((k, v) -> {
            Set<TypeConfigInfo> parseRet = parse(k, v);
            if (!CommonUtils.isEmpty(parseRet)) set.addAll(parseRet);
        });
        return set;
    }

}
