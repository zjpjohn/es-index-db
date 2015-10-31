package com.wxingyl.es.command.update;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractEntryPreQueryRtAction;
import com.wxingyl.es.command.RootNode;
import com.wxingyl.es.command.RtEsUtils;

/**
 * Created by xing on 15/10/10.
 * default update real time command action
 * <p/>
 * when {@link UpdateFieldEntry#beforeValue} is null, the default action of document map is put or remove
 * {@link UpdateFieldEntry#afterValue} value directly
 */
public class DefaultUpdateRtAction extends AbstractEntryPreQueryRtAction<UpdateFieldEntry> {

    public DefaultUpdateRtAction(IndexTypeInfo.TableInfo tableInfo) {
        this(tableInfo, 100);
    }

    public DefaultUpdateRtAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo, pageSize);
    }

    @Override
    protected void replaceDocField(Object parentMap, RootNode.Node node, UpdateFieldEntry entry) {
        RtEsUtils.replaceDocField(parentMap, node.field(), entry.getBeforeValue(), entry.getAfterValue());
    }

}
