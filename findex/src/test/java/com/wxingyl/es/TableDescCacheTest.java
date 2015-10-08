package com.wxingyl.es;

import com.wxingyl.es.util.TableDescCache;
import org.junit.Test;

/**
 * Created by xing on 15/10/8.
 * TableDescCache test
 */
public class TableDescCacheTest {

    @Test
    public void test() {
        TableDescCache.getTableDesc("auto", "order");
        TableDescCache.getTableDesc("auto", "name");
        TableDescCache.getTableDesc("auto", "user");
        TableDescCache.getTableDesc("sea", "info");
        TableDescCache.getTableDesc("sea", "goods");
        System.out.println("before gc: " + TableDescCache.asMap());
        System.gc();
        System.out.println("after gc: " + TableDescCache.asMap());
    }
}
