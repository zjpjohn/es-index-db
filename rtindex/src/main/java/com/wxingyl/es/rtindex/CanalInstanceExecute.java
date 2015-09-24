package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.exception.RtIndexDealException;
import com.wxingyl.es.index.IndexTypeBean;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by xing on 15/9/23.
 * canal instance executor
 */
public class CanalInstanceExecute implements Runnable {

    private CanalConnectorAdapter canalConnector;

    private volatile boolean running;

    private long startRtIndexTime;

    private StringCache StringCache = new StringCache();

//    private ConcurrentMap<IndexTypeBean, TypeRtIndexAction> typeRtActionMap = new ConcurrentHashMap<>();

    private ConcurrentMap<String, TableBaseInfo> tableInfoMap = new ConcurrentHashMap<>();

    private ConcurrentMap<String, List<TypeRtIndexAction>> tableRtActionMap = new ConcurrentHashMap<>();

    public CanalInstanceExecute(CanalConnectorAdapter canalConnector) {
        this.canalConnector = canalConnector;
    }

    @Override
    public void run() {
        if (tableRtActionMap.isEmpty()) return;
        canalConnector.connect();
        running = true;
        try {
            while (running) {
                Message message = canalConnector.getWithoutAck();
                try {
                    if (message.getId() > 0 && !message.getEntries().isEmpty()) {
                        for (CanalEntry.Entry e : message.getEntries()) {
                            CanalEntry.Header header = e.getHeader();
                            if (e.getEntryType() != CanalEntry.EntryType.ROWDATA ||
                                    header.getExecuteTime() <= startRtIndexTime ||
                                    !canalConnector.supportEventType(header.getEventType())) {
                                continue;
                            }
                            String table = StringCache.getString(header.getSchemaName(), header.getTableName());
                            if (!tableInfoMap.containsKey(table)) continue;
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(e.getStoreValue());
                            if (rowChange.getIsDdl() || !canalConnector.supportEventType(rowChange.getEventType())) continue;
                            //TODO real time index deal
                        }
                    }
                } catch (Throwable e) {
                    canalConnector.rollback(message.getId());
                    throw new RtIndexDealException("deal real time index: " + canalConnector.getDestination()
                            + " have exception", e);
                } finally {
                    canalConnector.ack(message.getId());
                }
            }
        } finally {
            canalConnector.disConnect();
            running = false;
            StringCache.clear();
        }
    }

    public void setStartRtIndexTime(long startRtIndexTime) {
        this.startRtIndexTime = startRtIndexTime;
    }

    public boolean isRunning() {
        return running;
    }

    public void registerTypeRtIndexAction(IndexTypeBean type, TypeRtIndexAction action) {
        Objects.requireNonNull(action);
        for (TableBaseInfo t : type.getAllTableInfo()) {
            String table = StringCache.getString(t.getTable().getSchema(), t.getTable().getTable());
            List<TypeRtIndexAction> list = tableRtActionMap.get(table);
            if (!tableInfoMap.containsKey(table)) {
                tableInfoMap.put(table, t);
                tableRtActionMap.put(table, list = new LinkedList<>());
            }
            if (!list.contains(action)) list.add(action);
        }
        if (tableInfoMap.size() >= StringCache.max) {
            StringCache.max = tableInfoMap.size() << 1;
        }
    }

    public void stop() {
        running = false;
    }

    private class StringCache {

        Map<String, Map<String, String>> cacheMap = new HashMap<>();

        int count;

        int max = 1024;

        String getString(String schema, String table) {
            Map<String, String> map = cacheMap.get(schema);
            if (map == null) {
                cacheMap.put(schema, map = new HashMap<>());
            }
            String desc = map.get(table);
            if (desc == null) {
                if (count > max) {
                    reduce();
                }
                map.put(table, desc = schema + '.' + table);
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
            for (Map<String, String> map : cacheMap.values()) {
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
