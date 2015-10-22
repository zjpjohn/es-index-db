package com.wxingyl.es.command.delete;

import org.elasticsearch.action.delete.DeleteRequestBuilder;

/**
 * Created by xing on 15/10/21.
 * single master table delete real time command
 */
public interface SingleMasterDeleteRtCommand extends MasterDeleteRtCommand {

    /**
     * return doc id value
     */
    DeleteRequestBuilder getDeleteRequest();

}
