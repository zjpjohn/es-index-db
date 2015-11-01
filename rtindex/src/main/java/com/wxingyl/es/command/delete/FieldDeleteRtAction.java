package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractEntryPreQueryRtAction;
import com.wxingyl.es.command.RootNode;
import com.wxingyl.es.command.RtEsUtils;

/**
 * Created by xing on 15/10/28.
 * assign null value action of real time
 */
public class FieldDeleteRtAction extends AbstractEntryPreQueryRtAction<DeleteFieldEntry> {

    public FieldDeleteRtAction(IndexTypeInfo.TableInfo tableInfo) {
        this(tableInfo, 100);
    }

    public FieldDeleteRtAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo, pageSize);
    }

    @Override
    protected void replaceDocField(Object parentMap, RootNode.Node node, DeleteFieldEntry entry) {
        RtEsUtils.replaceDocField(parentMap, node.field(), entry.getSrcValue(), null);
    }
}
