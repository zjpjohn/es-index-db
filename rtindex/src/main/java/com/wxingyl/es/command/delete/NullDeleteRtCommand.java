package com.wxingyl.es.command.delete;

import com.wxingyl.es.command.PreQueryRtCommand;

/**
 * Created by xing on 15/10/27.
 * slave table delete action, set some field to null
 */
public interface NullDeleteRtCommand extends PreQueryRtCommand {

    void addNullField(String fieldName);
}
