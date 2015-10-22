package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractRtCommand;
import com.wxingyl.es.command.MasterRtCommand;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.action.delete.DeleteRequestBuilder;

/**
 * Created by xing on 15/10/21.
 * master delete rtCommand action
 */
public class MasterDeleteRtCommandAction extends AbstractRtCommand implements MasterRtCommand {

    private final DeleteRequestBuilder requestBuilder;

    public MasterDeleteRtCommandAction(IndexTypeInfo.TableInfo tableInfo, String idVal) {
        super(tableInfo);
        requestBuilder = getClient().prepareDelete(tableInfo.getType().getIndex(),
                tableInfo.getType().getType(), idVal);
    }

    @Override
    public boolean isInvalid() {
        return CommonUtils.isEmpty(requestBuilder.request().id());
    }

    @Override
    public IndexTypeInfo.TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public DeleteRequestBuilder makeRequest() {
        return requestBuilder;
    }

}
