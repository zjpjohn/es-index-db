package com.wxingyl.es.command.insert;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.MasterRtCommand;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * BatchMasterInsertRtCommand Action
 */
public class BatchMasterInsertRtAction extends AbstractMasterInsertRtAction implements BatchMasterInsertRtCommand {

    private List<Map<String, Object>> rowsData = new LinkedList<>();

    public BatchMasterInsertRtAction(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
    }

    @Override
    protected List<Map<String, Object>> getTableResultData() {
        return rowsData;
    }

    @Override
    public boolean isInvalid() {
        return rowsData.isEmpty();
    }

    @Override
    public void mergeInsertRtCommand(SingleMasterInsertRtCommand rtCommand) {
        rowsData.add(rtCommand.getTableRow());
    }

    @Override
    public void mergeInsertRtCommand(BatchMasterInsertRtAction rtCommand) {
        rowsData.addAll(rtCommand.getRowsData());
    }

    @Override
    public boolean acceptMerge(MasterRtCommand rtCommand) {
        return tableInfo.equals(rtCommand.getTableInfo()) && !rtCommand.isInvalid();
    }

    @Override
    public List<Map<String, Object>> getRowsData() {
        return rowsData;
    }

}
