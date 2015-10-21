package com.wxingyl.es.command.insert;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * batch master insert RtCommand
 */
public interface BatchMasterInsertRtCommand extends MasterInsertRtCommand {

    void mergeInsertRtCommand(SingleMasterInsertRtCommand rtCommand);

    void mergeInsertRtCommand(BatchMasterInsertRtCommandAction rtCommand);

    boolean mergeAccept(MasterInsertRtCommand rtCommand);

    List<Map<String, Object>> getRowsData();

}
