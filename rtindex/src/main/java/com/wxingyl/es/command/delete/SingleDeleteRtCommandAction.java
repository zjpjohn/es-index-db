package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.command.AbstractRtCommand;
import com.wxingyl.es.util.CommonUtils;

/**
 * Created by xing on 15/10/21.
 * master delete rtCommand action
 */
public class SingleDeleteRtCommandAction extends AbstractRtCommand implements SingleDeleteRtCommand {


    private String idVal;

    public SingleDeleteRtCommandAction(IndexTypeInfo.TableInfo tableInfo, String idVal) {
        super(tableInfo);
        this.idVal = idVal;
    }

    @Override
    public boolean isInvalid() {
        return CommonUtils.isEmpty(idVal);
    }

    @Override
    public String getDocId() {
        return idVal;
    }
}
