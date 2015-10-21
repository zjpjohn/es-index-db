package com.wxingyl.es.command.delete;

/**
 * Created by xing on 15/10/21.
 * master table delete real time command
 */
public interface SingleDeleteRtCommand extends DeleteRtCommand {

    /**
     * return doc id value
     */
    String getDocId();
}
