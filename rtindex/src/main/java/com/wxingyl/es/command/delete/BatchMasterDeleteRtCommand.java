package com.wxingyl.es.command.delete;

import java.util.List;

/**
 * Created by xing on 15/10/22.
 * batch op master delete rt command
 */
public interface BatchMasterDeleteRtCommand extends MasterDeleteRtCommand {

    void mergeMasterDeleteRtCommand(SingleMasterDeleteRtCommand rtCommand);

}
