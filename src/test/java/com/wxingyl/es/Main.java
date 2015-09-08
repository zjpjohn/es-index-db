package com.wxingyl.es;

import com.wxingyl.es.conf.DefaultConfigManager;

/**
 * Created by xing on 15/8/11.
 * 创建项目
 */
public class Main {

    public static void main(String[] args) {
        DefaultConfigManager configManager = new DefaultConfigManager();
        configManager.parseDataSource("/Users/xing/code/db-river-elasticsearch/src/test/resources/datasource.yml");
        configManager.parseIndexType("/Users/xing/code/db-river-elasticsearch/src/test/resources/index_data.yml");
        System.out.println();
    }

}
