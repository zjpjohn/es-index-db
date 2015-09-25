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
        Map<String, List<CanalEntry.RowData>> tableGroupData = new LinkedHashMap<>();
        dealCanalMessage.orgTableGroupData = tableGroupData;
        try {
            while (running) {
                Message message = canalConnector.getWithoutAck();
                try {
                    if (message.getId() > 0 && !message.getEntries().isEmpty()) {
                        for (CanalEntry.Entry e : message.getEntries()) {
                            Tuple<String, List<CanalEntry.RowData>> tuple = canalConnector.filterEntry(e);
                            if (tuple == null) continue;
                            List<CanalEntry.RowData> list = tableGroupData.get(tuple.v1());
                            if (list == null) {
                                tableGroupData.put(tuple.v1(), list = new LinkedList<>());
                            }
                            list.addAll(tuple.v2());
                        }
                        typeActionInfoLock.readOp(dealCanalMessage);
                        for(List<CanalEntry.RowData> l : tableGroupData.values()) {
                            l.clear();
                        }
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
            tableGroupData.clear();
            //this must last
            canalConnector.disConnect();
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
        for (DbTableDesc table : supportTables) {
            actionInfo.putTableField(table);
        }
        typeActionInfoLock.writeOp(new Function<Set<TypeRtIndexActionInfo>, Void>() {
            @Override
            public Void apply(Set<TypeRtIndexActionInfo> input) {
                input.add(actionInfo);
                return null;
            }
        });
    }

    public void stop() {
        running = false;
    }

    private class TypeRtIndexActionInfo {

        TypeRtIndexAction action;

        IndexTypeDesc type;

        Map<String, List<String>> tableFieldMap = new HashMap<>();

        TypeRtIndexActionInfo(TypeRtIndexAction action, IndexTypeDesc type) {
            this.action = action;
            this.type = type;
        }

        void putTableField(DbTableDesc t) {
            String table = CommonUtils.tableToString(t);
            tableFieldMap.put(table, configManager.getTableFields(type, table));
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
        //key: schema.table
        Map<String, List<CanalEntry.RowData>> orgTableGroupData;

        Map<String, List<CanalEntry.RowData>> tableGroupData = new HashMap<>();

        @Override
        public Void apply(Set<TypeRtIndexActionInfo> actions) {

            for (TypeRtIndexActionInfo actionInfo : actions) {
                tableGroupData.clear();
                for (String table : orgTableGroupData.keySet()) {
//                    actionInfo.tableFieldMap
                }
                actionInfo.action.dealDataChange(instanceName, tableGroupData);
            }
            tableGroupData.clear();
            return null;
        }
    }
}
