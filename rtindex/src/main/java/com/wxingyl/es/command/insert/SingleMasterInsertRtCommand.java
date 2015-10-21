package com.wxingyl.es.command.insert;

import java.util.Map;

/**
 * Created by xing on 15/10/21.
 * master table insert record, we need query data from database
 */
public interface SingleMasterInsertRtCommand extends MasterInsertRtCommand {

    /**
     * @return get master table row data
     */
    Map<String, Object> getTableRow();

}
