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

    private String versionIndexName;

    public VersionIndexTypeBean(String versionIndexName) {
        this.versionIndexName = versionIndexName;
    }

    public void setIndexTypeBean(IndexTypeBean typeBean) {
        indexTypeBean = typeBean;
        aliasType = new IndexTypeDesc(versionIndexName, typeBean.getType().getType());
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

}
