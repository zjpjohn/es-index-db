package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.elasticsearch.common.collect.Tuple;

import java.util.List;

/**
 * Created by xing on 15/9/22.
 * canal connector adapt
 */
public interface CanalConnectorAdapter {

    String getDestination();

    void connect() throws CanalClientException;

    Message getWithoutAck() throws CanalClientException;

    void ack(long batchId) throws CanalClientException;

    void rollback(long batchId) throws CanalClientException;

    void disConnect() throws CanalClientException;

    Tuple<String, List<CanalEntry.RowData>> filterEntry(CanalEntry.Entry e) throws Exception;

    void setStartRtIndexTime(long startRtIndexTime);

}
