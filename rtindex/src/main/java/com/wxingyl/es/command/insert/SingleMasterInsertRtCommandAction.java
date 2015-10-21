package com.wxingyl.es.command.insert;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.util.CommonUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * master insert rt  command action
 */
public class SingleMasterInsertRtCommandAction extends AbstractMasterInsertRtCommand implements SingleMasterInsertRtCommand {

    private Map<String, Object> rowData;

    public SingleMasterInsertRtCommandAction(IndexTypeInfo.TableInfo tableInfo, Map<String, Object> rowData) {
        super(tableInfo);
        this.rowData = rowData;
    }

    @Override
    public boolean isInvalid() {
        return CommonUtils.isEmpty(rowData);
    }

    @Override
    public Map<String, Object> getTableRow() {
        return rowData;
    }

    @Override
    protected List<Map<String, Object>> getTableResultData() {
        return Collections.singletonList(rowData);
    }
}
