package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wxingyl.es.db.DbTableDesc;
import org.elasticsearch.common.collect.Tuple;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by xing on 15/9/22.
 * simple canal connector adapter
 */
public class SimpleCanalConnectorAdapter implements CanalConnectorAdapter {

    private static final Set<CanalEntry.EventType> SUPPORT_TYPES;

    static {
        Set<CanalEntry.EventType> set = new HashSet<>();
        set.add(CanalEntry.EventType.UPDATE);
        set.add(CanalEntry.EventType.DELETE);
        set.add(CanalEntry.EventType.INSERT);
        SUPPORT_TYPES = Collections.unmodifiableSet(set);
    }

    private String destination;

    private CanalConnector canalConnector;

    private DbTableCache dbTableCache = new DbTableCache();

    private int batchSize = 1000;
    /**
     * time unit is ms
     */
    private Long timeout = 500l;

    private long startRtIndexTime;

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
        dbTableCache.clear();
    }

    @Override
    public void setStartRtIndexTime(long startRtIndexTime) {
        this.startRtIndexTime = startRtIndexTime;
    }

    @Override
    public Tuple<DbTableDesc, List<CanalEntry.RowData>> filterEntry(CanalEntry.Entry e) throws InvalidProtocolBufferException {
        CanalEntry.Header header = e.getHeader();
        if (e.getEntryType() != CanalEntry.EntryType.ROWDATA
                || header.getExecuteTime() <= startRtIndexTime
                || !e.hasStoreValue()
                || !SUPPORT_TYPES.contains(header.getEventType())) {
            return null;
        }
        DbTableDesc table = dbTableCache.getTableStr(header.getSchemaName(), header.getTableName());
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(e.getStoreValue());
        if (rowChange.getIsDdl()) return null;
        return Tuple.tuple(table, rowChange.getRowDatasList());
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

    private class DbTableCache {

        Map<String, Map<String, DbTableDesc>> cacheMap = new HashMap<>();

        int count;

        int max = 1024;

        DbTableDesc getTableStr(String schema, String table) {
            Map<String, DbTableDesc> map = cacheMap.get(schema);
            if (map == null) {
                cacheMap.put(schema, map = new HashMap<>());
            }
            DbTableDesc desc = map.get(table);
            if (desc == null) {
                if (count > max) {
                    reduce();
                }
                map.put(table, desc = new DbTableDesc(schema, table));
                count++;
            }
            return desc;
        }

        void clear() {
            cacheMap.clear();
            count = 0;
        }

        void reduce() {
            Iterator<String> it;
            int delCount = 0;
            int min = max >> 1;
            for (Map<String, DbTableDesc> map : cacheMap.values()) {
                if (map.isEmpty() || delCount > min) continue;
                it = map.keySet().iterator();
                while (it.hasNext()) {
                    map.remove(it.next());
                    if (++delCount > min) break;
                }
            }
        }

    }
}
