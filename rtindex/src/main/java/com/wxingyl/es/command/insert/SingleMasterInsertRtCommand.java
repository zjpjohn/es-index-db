package com.wxingyl.es.command.insert;

import com.wxingyl.es.command.MasterRtCommand;

import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * master table insert record, we need query data from database
 */
public interface SingleMasterInsertRtCommand extends MasterRtCommand {

    /**
     * @return get master table row data
     */
    Map<String, Object> getTableRow();

}
