package com.wxingyl.es.canal;

import com.wxingyl.es.handle.RtIndexHandle;
import com.wxingyl.es.index.IndexTypeEvent;
import com.wxingyl.es.util.Listener;

import java.util.concurrent.ExecutorService;

/**
 * Created by xing on 15/10/8.
 * CanalInstanceExecute interface define
 */
public interface CanalInstanceExecute extends Runnable, Listener<IndexTypeEvent> {

    boolean isRunning();

    /**
     * register rtIndex action, if {@link RtIndexHandle#supportTable(String)} have change, you can recall this function,
     * it will replace action
     * @param action reIndex action, deal data change, if you recall this function, action obj should same obj of first call
     */
    void registerTypeRtIndexAction(RtIndexHandle action);

    /**
     * @return true: had replace, false: before can not find, curAction not add
     */
    boolean replaceTypeRtIndexAction(final RtIndexHandle before, RtIndexHandle curAction);

    void setExecutorService(ExecutorService executorService);

    void stop();
}
