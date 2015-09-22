package com.wxingyl.es.rindex;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Created by xing on 15/9/22.
 * simple canal connector adapter
 */
public class SimpleCanalConnectorAdapter implements CanalConnectorAdapt {

    private String destination;

    private CanalConnector canalConnector;

    private int batchSize = 1000;
    /**
     * time unit is ms
     */
    private Long timeout = 500l;

    public SimpleCanalConnectorAdapter(String destination, int port) {
        this(new InetSocketAddress(AddressUtils.getHostIp(), port), destination, "", "");
    }

    public SimpleCanalConnectorAdapter(SocketAddress address, String destination,
                                       String username, String password) {
        this.destination = destination;
        canalConnector = CanalConnectors.newSingleConnector(address, destination, username, password);
    }

    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public void connect() throws CanalClientException {
        canalConnector.connect();
        canalConnector.subscribe();
    }

    @Override
    public Message getWithoutAck() throws CanalClientException {
        return canalConnector.getWithoutAck(batchSize, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        canalConnector.ack(batchId);
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        if (batchId <= 0) canalConnector.rollback();
        else canalConnector.rollback(batchId);
    }

    @Override
    public void disConnect() throws CanalClientException {
        canalConnector.disconnect();
    }

    public Long getTimeout() {
        return timeout;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * time unit is ms
     */
    public void setTimeout(Long timeout) {
        Objects.requireNonNull(timeout);
        this.timeout = timeout;
    }
}
