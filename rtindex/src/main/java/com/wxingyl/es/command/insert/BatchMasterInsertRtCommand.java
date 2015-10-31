package com.wxingyl.es.command.insert;

import com.wxingyl.es.command.MasterRtCommand;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * batch master insert RtCommand
 */
public interface BatchMasterInsertRtCommand extends MasterRtCommand {

    void mergeInsertRtCommand(SingleMasterInsertRtCommand rtCommand);

    void mergeInsertRtCommand(BatchMasterInsertRtAction rtCommand);

    boolean acceptMerge(MasterRtCommand rtCommand);

    List<Map<String, Object>> getRowsData();

}
