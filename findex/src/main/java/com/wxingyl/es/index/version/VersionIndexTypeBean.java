package com.wxingyl.es.index.version;

import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.db.query.TableQueryInfo;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;

/**
 * Created by xing on 15/9/13.
 * wrapper indexTypeBean, support index alias
 */
public class VersionIndexTypeBean implements IndexTypeBean {

    private IndexTypeBean indexTypeBean;

    private IndexTypeDesc aliasType;

    private VersionIndex versionIndex;

    public VersionIndexTypeBean(VersionIndex versionIndex) {
        this.versionIndex = versionIndex;
    }

    public void setIndexTypeBean(IndexTypeBean typeBean) {
        indexTypeBean = typeBean;
        aliasType = new IndexTypeDesc(versionIndex.getVersionIndexName(), typeBean.getType().getType());
    }

    public VersionIndex getVersionIndex() {
        return versionIndex;
    }

    @Override
    public IndexTypeDesc getType() {
        return aliasType;
    }

    @Override
    public TableQueryInfo getMasterTable() {
        return indexTypeBean.getMasterTable();
    }

    @Override
    public List<TableBaseInfo> getTableInfo(String tableName) {
        return indexTypeBean.getTableInfo(tableName);
    }

    @Override
    public List<TableBaseInfo> getAllTableInfo() {
        return indexTypeBean.getAllTableInfo();
    }

    @Override
    public int getPriority() {
        return indexTypeBean.getPriority();
    }

    @Override
    public int compareTo(IndexTypeBean o) {
        return indexTypeBean.compareTo(o);
    }
}
