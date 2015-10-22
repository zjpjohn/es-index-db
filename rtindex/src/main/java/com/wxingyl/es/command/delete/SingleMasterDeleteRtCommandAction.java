package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.action.delete.DeleteRequestBuilder;

/**
 * Created by xing on 15/10/21.
 * master delete rtCommand action
 */
public class SingleMasterDeleteRtCommandAction extends AbstractMasterDeleteRtCommand implements SingleMasterDeleteRtCommand {


    private final DeleteRequestBuilder requestBuilder;

    public SingleMasterDeleteRtCommandAction(IndexTypeInfo.TableInfo tableInfo, String idVal) {
        super(tableInfo);
        requestBuilder = tableInfo.getIndexManager().getClient().prepareDelete(tableInfo.getType().getIndex(),
                tableInfo.getType().getType(), idVal);
    }

    @Override
    public boolean isInvalid() {
        return CommonUtils.isEmpty(requestBuilder.request().id());
    }

    @Override
    public int deleteDoc() {
        return getDeleteRequest().get().isFound() ? 1 : 0;
    }

    @Override
    public DeleteRequestBuilder getDeleteRequest() {
        return requestBuilder;
    }
}
