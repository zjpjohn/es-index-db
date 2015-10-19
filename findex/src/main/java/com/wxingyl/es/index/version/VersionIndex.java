package com.wxingyl.es.index.version;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

/**
 * Created by xing on 15/9/14.
 * index have version info
 */
public class VersionIndex implements Comparable<VersionIndex> {

    private int version;

    private String versionIndexName;

    private String indexName;

    private ImmutableOpenMap<String, MappingMetaData> mappings;

    private Settings settings;

    public VersionIndex(int version, String indexName) {
        this.version = version;
        this.indexName = indexName;
    }

    public VersionIndex(int version, String indexName, String versionIndexName) {
        this.version = version;
        this.indexName = indexName;
        this.versionIndexName = versionIndexName;
    }

    public void initConfig(ImmutableOpenMap<String, MappingMetaData> mappings, Settings settings) {
        this.mappings = mappings;
        this.settings = settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public ImmutableOpenMap<String, MappingMetaData> getMappings() {
        return mappings;
    }

    public Settings getSettings() {
        return settings;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getVersionIndexName() {
        if (versionIndexName == null) {
            versionIndexName = version == 0 ? indexName : indexName + "_v" + version;
        }
        return versionIndexName;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public int compareTo(VersionIndex o) {
        return Integer.compare(o.version, this.version);
    }

    @Override
    public String toString() {
        return "VersionIndex{" +
                "indexName='" + indexName + '\'' +
                ", version=" + version +
                '}';
    }
}
