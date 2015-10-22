package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;

/**
 * Created by xing on 15/10/21.
 * Master table RtCommand, it only use for DELETE and INSERT RtCommand
 */
public interface MasterRtCommand extends RtCommand {

    IndexTypeInfo.TableInfo getTableInfo();
}
