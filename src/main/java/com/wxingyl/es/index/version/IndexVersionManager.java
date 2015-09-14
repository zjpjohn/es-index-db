package com.wxingyl.es.index.version;

import java.util.Set;

/**
 * Created by xing on 15/9/14.
 * index version manager
 */
public interface IndexVersionManager {

    VersionIndexTypeBean getIndexVersion(String indexName);

    /**
     * @return support index name. return null, it mean support all index
     */
    Set<String> supportIndex();
}
