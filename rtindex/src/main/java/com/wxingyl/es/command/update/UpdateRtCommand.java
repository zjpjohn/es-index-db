package com.wxingyl.es.command.update;

import com.wxingyl.es.command.ModifiableRtCommand;

/**
 * Created by xing on 15/10/8.
 * update RtCommand
 */
public interface UpdateRtCommand extends ModifiableRtCommand {

    void addChangeField(ChangedFieldEntry entry);

    boolean needContinue();

}
