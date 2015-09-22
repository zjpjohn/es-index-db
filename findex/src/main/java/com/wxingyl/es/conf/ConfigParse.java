package com.wxingyl.es.conf;

import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/8/24.
 * config parse interface
 */
public interface ConfigParse<T> {

    Set<T> parse(String name, Map<String, Object> config);
}
