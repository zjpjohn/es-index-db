package com.wxingyl.es.rindex;

import com.alibaba.otter.canal.protocol.Message;

/**
 * Created by xing on 15/9/23.
 *  canal instance executor
 */
public class CanalInstanceExecute implements Runnable {

    private CanalConnectorAdapter canalConnector;

    private volatile boolean running;

    public CanalInstanceExecute(CanalConnectorAdapter canalConnector) {
        this.canalConnector = canalConnector;
    }


    @Override
    public void run() {
        canalConnector.connect();
        running = true;
        try {
            while (running) {
                Message message = canalConnector.getWithoutAck();
                if (message.getId() > 0 && !message.getEntries().isEmpty()) {

                }
                canalConnector.ack(message.getId());
            }
        } finally {
            canalConnector.disConnect();
            running = false;
        }
    }

    public boolean isRunning() {
        return running;
    }

}
