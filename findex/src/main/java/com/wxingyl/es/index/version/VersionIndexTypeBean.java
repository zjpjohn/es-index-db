package com.wxingyl.es.index.version;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.util.TypeDescCache;

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
        aliasType = TypeDescCache.getTypeDesc(versionIndex.getVersionIndexName(), typeBean.getType().getType());
    }

    public VersionIndex getVersionIndex() {
        return versionIndex;
    }

    public IndexTypeDesc getSrcType() {
        return indexTypeBean.getType();
    }

    @Override
    public IndexTypeDesc getType() {
        return aliasType;
    }

    @Override
    public TableQueryBean getMasterTable() {
        return indexTypeBean.getMasterTable();
    }

    @Override
    public SqlQueryCommon getTableQueryInfo(DbTableDesc table) {
        return indexTypeBean.getTableQueryInfo(table);
    }

    @Override
    public List<SqlQueryCommon> getAllTableQueryInfo() {
        return indexTypeBean.getAllTableQueryInfo();
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
