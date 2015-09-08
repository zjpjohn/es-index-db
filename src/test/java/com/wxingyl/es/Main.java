package com.wxingyl.es;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.DefaultConfigManager;

/**
 * Created by xing on 15/8/11.
 * 创建项目
 */
public class Main {

    private static final String DS_CONFIG_FILE = "/Users/xing/code/db-river-elasticsearch/src/test/resources/datasource.yml";

    private static final String INDEX_CONFIG_FILE = "/Users/xing/code/db-river-elasticsearch/src/test/resources/index_data.yml";

    public static void main(String[] args) {
        ConfigManager configManager = new DefaultConfigManager();
        configManager.parseDataSource(DS_CONFIG_FILE);
        configManager.parseIndexType(INDEX_CONFIG_FILE);
        System.out.println(configManager.getIndexTypeBean("order_v1"));
    }

}
