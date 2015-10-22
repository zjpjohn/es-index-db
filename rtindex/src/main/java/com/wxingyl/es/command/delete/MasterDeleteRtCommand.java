package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.IndexTypeInfo;

/**
 * Created by xing on 15/10/22.
 * Master table real time command
 */
public interface MasterDeleteRtCommand extends DeleteRtCommand {

    /**
     * @return delete doc num
     */
    int deleteDoc();

    IndexTypeInfo.TableInfo getTableInfo();
}
