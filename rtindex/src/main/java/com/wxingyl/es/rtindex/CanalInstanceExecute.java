package com.wxingyl.es.rtindex;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.exception.RtIndexDealException;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.RwLock;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Supplier;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Tuple;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * Created by xing on 15/9/23.
 * canal instance executor
 * can not run multi-threads
 */
public class CanalInstanceExecute implements Runnable {

    private CanalConnectorAdapter canalConnector;

    private volatile boolean running;

    private RtIndexConfigManager configManager;

    private DealCanalMessage dealCanalMessage = new DealCanalMessage();

    private String instanceName;

    private ExecutorService executorService;

    private RwLock<Set<TypeRtIndexActionInfo>> typeActionInfoLock = CommonUtils.createRwLock(new Supplier<Set<TypeRtIndexActionInfo>>() {
        @Override
        public Set<TypeRtIndexActionInfo> get() {
            return new HashSet<>();
        }
    });

    public CanalInstanceExecute(CanalConnectorAdapter canalConnector, RtIndexConfigManager configManager) {
        this.canalConnector = canalConnector;
        this.configManager = configManager;
        instanceName = canalConnector.getDestination();
    }

    @Override
    public void run() {
        if (typeActionInfoLock.readOp(new Function<Set<TypeRtIndexActionInfo>, Boolean>() {
            @Override
            public Boolean apply(Set<TypeRtIndexActionInfo> input) {
                return input.isEmpty();
            }
        })) return;
        canalConnector.connect();
        running = true;
        //here need update
        instanceName = canalConnector.getDestination();
        try {
            while (running) {
                Message message = canalConnector.getWithoutAck();
                try {
                    if (message.getId() > 0 && !message.getEntries().isEmpty()) {
                        for (CanalEntry.Entry e : message.getEntries()) {
                            Tuple<DbTableDesc, List<CanalEntry.RowData>> tuple = canalConnector.filterEntry(e);
                            if (tuple == null) continue;
                            dealCanalMessage.dataList.add(new ChangeDataEntry(tuple.v1(), e.getHeader().getEventType(), tuple.v2()));
                        }
                        typeActionInfoLock.readOp(dealCanalMessage);
                        dealCanalMessage.dataList.clear();
                    }
                    canalConnector.ack(message.getId());
                } catch (Throwable e) {
                    canalConnector.rollback(message.getId());
                    throw new RtIndexDealException("deal real time index: " + canalConnector.getDestination()
                            + " have exception", e);
                }
            }
        } finally {
            running = false;
            dealCanalMessage.dataList.clear();
            dealCanalMessage.callableList.clear();
            //this must last
            canalConnector.disConnect();
            typeActionInfoLock.readOp(new Function<Set<TypeRtIndexActionInfo>, Void>() {
                @Override
                public Void apply(Set<TypeRtIndexActionInfo> input) {
                    for (TypeRtIndexActionInfo action : input) {
                        action.actionData.clear();
                        action.haveData = false;
                    }
                    return null;
                }
            });
        }
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * register rtIndex action, if {@link TypeRtIndexAction#supportTable(String)} have change, you can recall this function,
     * it will replace action
     * @param action reIndex action, deal data change, if you recall this function, action obj should same obj of first call
     */
    public void registerTypeRtIndexAction(TypeRtIndexAction action) {
        Objects.requireNonNull(action);
        IndexTypeBean type = action.supportType(instanceName);
        Objects.requireNonNull(type);
        List<DbTableDesc> supportTables = action.supportTable(instanceName);
        if (CommonUtils.isEmpty(supportTables)) {
            supportTables = Lists.transform(type.getAllTableInfo(), new Function<TableBaseInfo, DbTableDesc>() {
                @Override
                public DbTableDesc apply(TableBaseInfo input) {
                    return input.getTable();
                }
            });
        }
        final TypeRtIndexActionInfo actionInfo = new TypeRtIndexActionInfo(action, type.getType());
        actionInfo.putTableField(supportTables);
        typeActionInfoLock.writeOp(new Function<Set<TypeRtIndexActionInfo>, Void>() {
            @Override
            public Void apply(Set<TypeRtIndexActionInfo> input) {
                input.add(actionInfo);
                return null;
            }
        });
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public void stop() {
        running = false;
    }

    private class TypeRtIndexActionInfo implements Callable<Void> {

        TypeRtIndexAction action;

        IndexTypeDesc type;

        Map<DbTableDesc, List<String>> tableFieldMap = new HashMap<>();

        Map<DbTableDesc, List<ChangeDataEntry>> actionData = new HashMap<>();

        private boolean haveData;

        @Override
        public Void call() throws Exception {
            try {
                action.dealDataChange(instanceName, actionData);
            } finally {
                for (List<ChangeDataEntry> list : actionData.values()) {
                    list.clear();
                }
                haveData = false;
            }
            return null;
        }

        TypeRtIndexActionInfo(TypeRtIndexAction action, IndexTypeDesc type) {
            this.action = action;
            this.type = type;
        }

        void addActionData(DbTableDesc table, ChangeDataEntry entry) {
            List<ChangeDataEntry> data = actionData.get(table);
            if (data == null) {
                actionData.put(table, data = new LinkedList<>());
            }
            data.add(entry);
            haveData = true;
        }

        void putTableField(List<DbTableDesc> supportTables) {
            for (DbTableDesc t : supportTables) {
                List<String> fields = configManager.getTableFields(type, t);
                if (fields != null) {
                    tableFieldMap.put(t, fields);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TypeRtIndexActionInfo)) return false;

            TypeRtIndexActionInfo that = (TypeRtIndexActionInfo) o;

            return action.equals(that.action);

        }

        @Override
        public int hashCode() {
            return action.hashCode();
        }
    }

    private class DealCanalMessage implements Function<Set<TypeRtIndexActionInfo>, Void> {

        List<ChangeDataEntry> dataList = new LinkedList<>();

        List<Callable<Void>> callableList = new ArrayList<>();

        private void initActionData(TypeRtIndexActionInfo actionInfo) {
            for (ChangeDataEntry e : dataList) {
                DbTableDesc table = e.getTable();
                List<String> fields = actionInfo.tableFieldMap.get(e.getTable());
                if (fields == null) continue;
                if (e.getEventType() == CanalEntry.EventType.UPDATE && !fields.isEmpty()) {
                    List<CanalEntry.RowData> dataList = null;
                    for (CanalEntry.RowData r : e.getRowData()) {
                        boolean need = false;
                        for (CanalEntry.Column c : r.getAfterColumnsList()) {
                            if (fields.contains(c.getName()) && c.getUpdated()) {
                                need = true;
                                break;
                            }
                        }
                        if (!need) {
                            if (dataList == null) {
                                dataList = new LinkedList<>(e.getRowData());
                            }
                            dataList.remove(r);
                        }
                    }
                    if (dataList == null) {
                        actionInfo.addActionData(table, e);
                    } else if (!dataList.isEmpty()) {
                        actionInfo.addActionData(table, new ChangeDataEntry(table, CanalEntry.EventType.UPDATE,
                                Collections.unmodifiableList(dataList)));
                    }
                } else {
                    actionInfo.addActionData(table, e);
                }
            }
        }

        @Override
        public Void apply(Set<TypeRtIndexActionInfo> actions) {
            //init action
            for (TypeRtIndexActionInfo actionInfo : actions) {
                initActionData(actionInfo);
                if (actionInfo.haveData) {
                    callableList.add(actionInfo);
                }
            }
            if (callableList.isEmpty()) return null;
            try {
                if (executorService == null) {
                    for (Callable<Void> call : callableList) {
                        call.call();
                    }
                } else {
                    executorService.invokeAll(callableList);
                }
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
                callableList.clear();
            }
            return null;
        }
    }
}
