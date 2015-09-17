package com.wxingyl.es.conf.index;

import com.wxingyl.es.conf.ConfigParse;

import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/9/8.
 * index config parse
 */
public interface IndexConfigParse extends ConfigParse<TypeConfigInfo> {

    Set<TypeConfigInfo> parseAll(Map<String, Map<String, Object>> configMap);

}
