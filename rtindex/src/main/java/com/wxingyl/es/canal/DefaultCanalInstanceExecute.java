package com.wxingyl.es.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.wxingyl.es.action.RtIndexAction;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.exception.RtIndexDealException;
import com.wxingyl.es.index.*;
import com.wxingyl.es.index.db.SqlQueryCommon;
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
public class DefaultCanalInstanceExecute implements CanalInstanceExecute {

    private CanalConnectorAdapter canalConnector;

    private volatile boolean running;

    private RtIndexConfigManager configManager;

    private DealCanalMessage dealCanalMessage = new DealCanalMessage();

    private String instanceName;

    private ExecutorService executorService;

    private IndexManager indexManager;

    private RwLock<Set<TypeRtIndexActionInfo>> typeActionInfoLock = CommonUtils.createRwLock(new Supplier<Set<TypeRtIndexActionInfo>>() {
        @Override
        public Set<TypeRtIndexActionInfo> get() {
            return new HashSet<>();
        }
    });

    public DefaultCanalInstanceExecute(CanalConnectorAdapter canalConnector, IndexManager indexManager) {
        this.canalConnector = canalConnector;
        this.configManager = (RtIndexConfigManager) indexManager.getConfigManager();
        this.indexManager = indexManager;
        instanceName = canalConnector.getDestination();
        this.indexManager.registerIndexEventListener(this);
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
        instanceName = canalConnector.getDestination();
        try {
            while (running) {
                typeActionInfoLock.readOp(dealCanalMessage);
            }
        } finally {
            running = false;
            dealCanalMessage.dataList.clear();
            dealCanalMessage.callableList.clear();
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
            //at last is a good choice
            canalConnector.disConnect();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /**
     * register rtIndex action, if {@link RtIndexAction#supportTable(String)} have change, you can recall this function,
     * it will replace action
     * @param action reIndex action, deal data change, if you recall this function, action obj should same obj of first call
     */
    @Override
    public void registerTypeRtIndexAction(RtIndexAction action) {
        final TypeRtIndexActionInfo actionInfo = initActionInfo(action);
        typeActionInfoLock.writeOp(new Function<Set<TypeRtIndexActionInfo>, Void>() {
            @Override
            public Void apply(Set<TypeRtIndexActionInfo> input) {
                input.add(actionInfo);
                return null;
            }
        });
    }

    /**
     * @return true: had replace, false: before can not find, curAction not add
     */
    @Override
    public boolean replaceTypeRtIndexAction(final RtIndexAction before, RtIndexAction curAction) {
        final TypeRtIndexActionInfo actionInfo = initActionInfo(curAction);
        return typeActionInfoLock.writeOp(new Function<Set<TypeRtIndexActionInfo>, Boolean>() {
            @Override
            public Boolean apply(Set<TypeRtIndexActionInfo> input) {
                TypeRtIndexActionInfo rmObj = null;
                for (TypeRtIndexActionInfo e : input) {
                    if (e.action.equals(before)) {
                        rmObj = e;
                        break;
                    }
                }
                if (rmObj == null) return false;
                else {
                    input.remove(rmObj);
                    input.add(actionInfo);
                    return true;
                }
            }
        });
    }

    @Override
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void onChange(IndexTypeEvent message) {
        final IndexTypeDesc type = message.getType();
        final IndexTypeEventTypeEnum eventType = message.getEventType();
        //we only care INDEX.START_CREATE and INDEX.END_CREATE
        if (eventType != IndexTypeEventTypeEnum.START_CREATE && eventType != IndexTypeEventTypeEnum.END_CREATE) return;
        typeActionInfoLock.readOp(new Function<Set<TypeRtIndexActionInfo>, Void>() {
            @Override
            public Void apply(Set<TypeRtIndexActionInfo> input) {
                for (TypeRtIndexActionInfo info : input) {
                    if (info.type.equals(type)) {
                        info.pause = eventType == IndexTypeEventTypeEnum.START_CREATE;
                        break;
                    }
                }
                return null;
            }
        });
    }

    private TypeRtIndexActionInfo initActionInfo(RtIndexAction action) {
        Objects.requireNonNull(action);
        IndexTypeBean type = action.supportType(instanceName);
        Objects.requireNonNull(type);
        List<DbTableDesc> supportTables = action.supportTable(instanceName);
        Objects.requireNonNull(supportTables);
        if (supportTables.isEmpty()) {
            supportTables = Lists.transform(type.getAllTableQueryInfo(), new Function<SqlQueryCommon, DbTableDesc>() {
                @Override
                public DbTableDesc apply(SqlQueryCommon input) {
                    return input.getTable();
                }
            });
        }
        TypeRtIndexActionInfo actionInfo = new TypeRtIndexActionInfo(action, type.getType());
        actionInfo.putTableField(supportTables);
        return actionInfo;
    }

    private class TypeRtIndexActionInfo implements Callable<Void> {

        RtIndexAction action;

        IndexTypeDesc type;

        Map<DbTableDesc, List<String>> tableFieldMap = new HashMap<>();

        Map<DbTableDesc, List<ChangeDataEntry>> actionData = new HashMap<>();

        private boolean haveData;

        private volatile boolean pause;

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

        TypeRtIndexActionInfo(RtIndexAction action, IndexTypeDesc type) {
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

        /**
         * if change field is not exist our document field, we no need to handle this change
         */
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
            Message message = canalConnector.getWithoutAck();
            try {
                if (message.getId() > 0 && !message.getEntries().isEmpty()) {
                    for (CanalEntry.Entry e : message.getEntries()) {
                        Tuple<DbTableDesc, List<CanalEntry.RowData>> tuple = canalConnector.filterEntry(e);
                        if (tuple == null) continue;
                        dataList.add(new ChangeDataEntry(tuple.v1(), e.getHeader().getEventType(), tuple.v2()));
                    }
                    //init action
                    for (TypeRtIndexActionInfo actionInfo : actions) {
                        if (actionInfo.pause) continue;
                        initActionData(actionInfo);
                        if (actionInfo.haveData) {
                            callableList.add(actionInfo);
                        }
                    }
                    if (!callableList.isEmpty()) {
                        if (executorService == null || callableList.size() == 1) {
                            for (Callable<Void> call : callableList) {
                                call.call();
                            }
                        } else {
                            executorService.invokeAll(callableList);
                        }
                    }
                }
                canalConnector.ack(message.getId());
            } catch (Throwable e) {
                canalConnector.rollback(message.getId());
                throw new RtIndexDealException("deal real time index: " + instanceName
                        + " have exception", e);
            } finally {
                callableList.clear();
                dataList.clear();
            }
            return null;
        }
    }
}
