package com.wxingyl.es.index.version;

import java.util.Set;

/**
 * Created by xing on 15/9/14.
 * index version manager
 */
public interface IndexVersionManager {

    /**
     * get top version index bean, if return is null, will call {@link DefaultIndexVersionManager#topVersionIndex(String)}
     * Note: if return VersionIndexTypeBean is null, and defaultIndexVersionManager is enable, will call {@link DefaultIndexVersionManager#topVersionIndex(String)}
     * @param indexName index name
     * @return VersionIndexTypeBean, if {@link VersionIndexTypeBean#getVersionIndex()} is null, it mean that indexName
     *  is not exist in elasticsearch, if return VersionIndexTypeBean is null, will call {@link DefaultIndexVersionManager#topVersionIndex(String)}
     */
    VersionIndexTypeBean topVersionIndex(String indexName);

    /**
     * a new index, we need to create it
     * Note: if return VersionIndex is null, and defaultIndexVersionManager is enable, will call {@link DefaultIndexVersionManager#createNewIndex(String)}
     * @return VersionIndex object, and it version should is v1
     */
    VersionIndex createNewIndex(String indexName);

    /**
     * Note: if return VersionIndex is null, and defaultIndexVersionManager is enable, will call {@link DefaultIndexVersionManager#createNextVersionIndex(VersionIndex)}
     * @param curTopVersion current max version index
     * @return VersionIndex object, and it version should is (curTopVersion.version + 1)
     */
    VersionIndex createNextVersionIndex(VersionIndex curTopVersion);

    /**
     * @return support index name. return null, it mean support all index
     */
    Set<String> supportIndex();
}
