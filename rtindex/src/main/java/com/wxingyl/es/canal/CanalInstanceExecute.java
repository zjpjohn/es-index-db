package com.wxingyl.es.canal;

import com.wxingyl.es.action.RtIndexAction;
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
     * register rtIndex action, if {@link RtIndexAction#supportTable(String)} have change, you can recall this function,
     * it will replace action
     * @param action reIndex action, deal data change, if you recall this function, action obj should same obj of first call
     */
    void registerTypeRtIndexAction(RtIndexAction action);

    /**
     * @return true: had replace, false: before can not find, curAction not add
     */
    boolean replaceTypeRtIndexAction(final RtIndexAction before, RtIndexAction curAction);

    void setExecutorService(ExecutorService executorService);

    void stop();
}
