package com.wxingyl.es.command.update;

import com.wxingyl.es.command.PreQueryRtCommand;

/**
 * Created by xing on 15/10/8.
 * update RtCommand
 */
public interface UpdateRtCommand extends PreQueryRtCommand {

    void addChangeField(ChangedFieldEntry entry);

}
