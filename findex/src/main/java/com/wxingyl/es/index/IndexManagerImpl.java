package com.wxingyl.es.index;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.index.doc.DefaultIndexDocTransfer;
import com.wxingyl.es.index.doc.IndexDocTransfer;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.index.doc.PageDocumentIterator;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
import com.wxingyl.es.index.generator.DefaultBulkIndexGenerator;
import com.wxingyl.es.index.version.DefaultIndexVersionManager;
import com.wxingyl.es.index.version.IndexVersionManager;
import com.wxingyl.es.index.version.VersionIndex;
import com.wxingyl.es.index.version.VersionIndexTypeBean;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Listener;
import com.wxingyl.es.util.RwLock;
import com.wxingyl.es.util.TypeDescCache;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Supplier;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by xing on 15/9/14.
 * index manager
 */
public class IndexManagerImpl implements IndexManager {

    private Client client;

    private IndexDocTransfer indexDocTransfer;

    private ConfigManager configManager;

    private BulkIndexGenerate defaultBulkIndexGenerator;

    private Map<IndexTypeDesc, BulkIndexGenerate> bulkIndexGeneratorMap = new HashMap<>();

    /**
     * switch default copy settings and mapping config
     * default turn on
     */
    private volatile boolean defaultIndexVersionManagerEnable = false;

    private ExecutorService executorService;

    private IndexVersionManager defaultIndexVersionManager;

    private Map<String, IndexVersionManager> indexVersionManagerMap = new HashMap<>();

    private RwLock<List<String>> fillingIndex = CommonUtils.createRwLock(new Supplier<List<String>>() {
        @Override
        public List<String> get() {
            return new LinkedList<>();
        }
    });

    private List<Listener<IndexTypeEvent>> indexEventListener = new ArrayList<>();

    public IndexManagerImpl(Client client, ConfigManager configManager) {
        this(client, configManager, new DefaultIndexDocTransfer());
    }

    public IndexManagerImpl(Client client, ConfigManager configManager, IndexDocTransfer indexDocTransfer) {
        this.client = client;
        this.indexDocTransfer = indexDocTransfer;
        this.configManager = configManager;
        defaultBulkIndexGenerator = new DefaultBulkIndexGenerator();
        switchDefaultIndexVersionManager(true);
    }

    @Override
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void switchDefaultIndexVersionManager(boolean turnOn) {
        if (defaultIndexVersionManagerEnable == turnOn) return;
        defaultIndexVersionManagerEnable = turnOn;
        if (turnOn && defaultIndexVersionManager == null) {
            defaultIndexVersionManager = new DefaultIndexVersionManager(client);
        }
    }

    @Override
    public void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate) {
        if (CommonUtils.isEmpty(bulkIndexGenerate.supportType())) {
            throw new IndexIllegalArgumentException("BulkIndexGenerate: " + bulkIndexGenerate + " supportType is empty");
        }
        if (isCreatingIndex()) {
            throw new IllegalStateException("now creating index, can not register BulkIndexGenerate");
        }
        for (IndexTypeDesc v : bulkIndexGenerate.supportType()) {
            if (v == null) return;
            bulkIndexGeneratorMap.put(v, bulkIndexGenerate);
        }
    }

    @Override
    public void registerIndexVersionManager(IndexVersionManager indexVersionManager) {
        String name = CommonUtils.emptyTrim(indexVersionManager.supportIndex());
        if (name == null) {
            throw new IndexIllegalArgumentException("IndexVersionManager: " + indexVersionManager + " supportIndex is empty");
        }
        if (isCreatingIndex()) {
            throw new IllegalStateException("now creating index, can not register IndexVersionManager");
        }
        indexVersionManagerMap.put(name, new IndexVersionManagerWrapper(indexVersionManager));
    }

    @Override
    public void registerIndexEventListener(Listener<IndexTypeEvent> listener) {
        indexEventListener.add(listener);
    }

    @Override
    public Client getClient() {
        return client;
    }

    @Override
    public ConfigManager getConfigManager() {
        return configManager;
    }

    @Override
    public IndexDocTransfer getIndexDocTransfer() {
        return indexDocTransfer;
    }

    @Override
    public BulkIndexGenerate getBulkIndexGenerate(IndexTypeDesc type) {
        return CommonUtils.getOrDefault(bulkIndexGeneratorMap, type, defaultBulkIndexGenerator);
    }

    @Override
    public long indexFill(String index, String type) {
        Objects.requireNonNull(type);
        Map<String, Long> numMap = indexFill(index, type, 1);
        if (numMap == null) return 0;
        else return CommonUtils.getOrDefault(numMap, type, 0l);
    }

    @Override
    public Map<String, Long> indexFill(String index) {
        return indexFill(index, null, 1);
    }

    /**
     * @param index         index name
     * @param type          if type == null, it mean create all type below the index
     * @param concurrentNum concurrent thread num, if num > 1, executorService must not null
     * @return Map, key: type, value: document total num
     */
    @Override
    public Map<String, Long> indexFill(final String index, String type, int concurrentNum) {
        Objects.requireNonNull(index);
        if (concurrentNum < 1) concurrentNum = 1;
        if (concurrentNum > 1 && executorService == null) {
            throw new IllegalStateException("concurrentNum = " + concurrentNum + ", but executorService is null");
        }
        Set<IndexTypeBean> typeBeanSet;
        if (type == null) {
            typeBeanSet = configManager.findIndexTypeBean(index);
        } else {
            IndexTypeBean bean = configManager.findIndexTypeBean(TypeDescCache.getTypeDesc(index, type));
            if (bean != null) {
                typeBeanSet = new HashSet<>();
                typeBeanSet.add(bean);
            } else {
                typeBeanSet = null;
            }
        }

        if (CommonUtils.isEmpty(typeBeanSet)) return null;

        if (fillingIndex.readOp(new Function<List<String>, Boolean>() {
            @Override
            public Boolean apply(List<String> input) {
                return input.contains(index);
            }
        })) {
            return null;
        }
        fillingIndex.writeOp(new Function<List<String>, Object>() {
            @Override
            public Object apply(List<String> input) {
                input.add(index);
                return null;
            }
        });

        VersionIndexTypeBean topVersionIndex = getVersionIndex(index);
        final VersionIndexTypeBean createVersionIndex;
        if (topVersionIndex != null) {
            createVersionIndex = createNextVersionIndex(index, topVersionIndex);
        } else {
            createVersionIndex = null;
        }
        Map<String, Long> typeDocNum = new HashMap<>();
        try {
            for (IndexTypeBean typeBean : typeBeanSet) {
                IndexTypeDesc orgType = typeBean.getType();
                IndexTypeEvent startEvent = new IndexTypeEvent(orgType, IndexTypeEventTypeEnum.START_CREATE);
                for (Listener<IndexTypeEvent> eventListener : indexEventListener) {
                    eventListener.onChange(startEvent);
                }
                if (createVersionIndex != null) {
                    createVersionIndex.setIndexTypeBean(typeBean);
                    typeBean = createVersionIndex;
                }
                long num;
                if (concurrentNum == 1) {
                    num = createIndex(typeBean, 0, 0);
                } else {
                    num = createIndexConcurrent(typeBean, concurrentNum);
                }
                typeDocNum.put(typeBean.getType().getType(), num);
                IndexTypeEvent endEvent = new IndexTypeEvent(orgType, IndexTypeEventTypeEnum.START_CREATE);
                for (Listener<IndexTypeEvent> eventListener : indexEventListener) {
                    eventListener.onChange(endEvent);
                }
            }
        } finally {
            fillingIndex.writeOp(new Function<List<String>, Object>() {
                @Override
                public Object apply(List<String> input) {
                    input.remove(index);
                    return null;
                }
            });
        }
        return typeDocNum;
    }

    private long createIndexConcurrent(final IndexTypeBean typeBean, int concurrentNum) {
        long totalNum;
        SqlQueryCommon masterCommon = typeBean.getMasterTable().getQueryCommon();
        try {
            totalNum = typeBean.getMasterTable().getQueryHandler().countKeyField(masterCommon);
        } catch (SQLException e) {
            throw new IndexDocException("count num of master table: " + typeBean.getMasterTable().getQueryCommon()
                    + " have sqlException", e);
        }
        long unitNum = totalNum / concurrentNum;
        int pageSize = masterCommon.getPageSize();
        //if every unit num < pageSize, don't need multi thread
        if (unitNum < pageSize) return createIndex(typeBean, 0, 0);
        int totalPage = totalNum / pageSize + totalNum % pageSize == 0 ? 0 : 1;
        int unitPageNum = totalPage / concurrentNum + totalPage % concurrentNum == 0 ? 0 : 1;
        int startPage = 0;
        List<Callable<Long>> callableList = new ArrayList<>(concurrentNum);
        for (int i = 0; i < concurrentNum; i++) {
            final int finalStartPage = startPage;
            final int finalEndPage = startPage + unitPageNum;
            callableList.add(new Callable<Long>() {

                @Override
                public Long call() throws Exception {
                    return createIndex(typeBean, finalStartPage, finalEndPage);
                }
            });
            startPage = finalEndPage;
        }
        try {
            List<Future<Long>> futureList = executorService.invokeAll(callableList);
            long ret = 0;
            for (Future<Long> f : futureList) {
                try {
                    ret += f.get();
                } catch (ExecutionException e) {
                    throw new IndexDocException("multi thread finish create index, get result have ExecutionException", e);
                }
            }
            return ret;
        } catch (InterruptedException e) {
            throw new IndexDocException("multi thread create index have InterruptedException", e);
        }
    }

    private long createIndex(IndexTypeBean typeBean, int startPage, int endPage) {
        PageDocumentIterator docItr = null;
        try {
            docItr = indexDocTransfer.indexDocCreate(typeBean, startPage, endPage);
            docItr.startFillIndex();
            long num = 0;
            while (docItr.hasNext()) {
                PageDocument document = docItr.next();
                num += CommonUtils.getOrDefault(bulkIndexGeneratorMap, document.getBaseInfo().getType(),
                        defaultBulkIndexGenerator).bulkInsert(client, document);
            }
            return num;
        } finally {
            if (docItr != null) docItr.finishFillIndex();
        }
    }

    private VersionIndexTypeBean getVersionIndex(String index) {
        IndexVersionManager manager = getIndexVersionManager(index);
        if (manager == null) {
            return null;
        } else {
            return manager.topVersionIndex(index);
        }
    }

    private IndexVersionManager getIndexVersionManager(String index) {
        IndexVersionManager manager = indexVersionManagerMap.get(index);
        return manager != null ? manager : defaultIndexVersionManagerEnable ? defaultIndexVersionManager : null;
    }

    private VersionIndexTypeBean createNextVersionIndex(String index, VersionIndexTypeBean topVersionIndex) {
        IndexVersionManager manager = getIndexVersionManager(index);
        if (manager == null) return null;
        VersionIndex topVersion = topVersionIndex.getVersionIndex();
        VersionIndex nextVersion;
        if (topVersion == null) {
            nextVersion = manager.createNewIndex(index);
        } else {
            nextVersion = manager.createNextVersionIndex(topVersion);
        }
        Objects.requireNonNull(nextVersion);
        return new VersionIndexTypeBean(nextVersion);
    }


    private boolean isCreatingIndex() {
        return fillingIndex.readOp(new Function<List<String>, Boolean>() {
            @Override
            public Boolean apply(List<String> input) {
                return !input.isEmpty();
            }
        });
    }

    class IndexVersionManagerWrapper implements IndexVersionManager {

        IndexVersionManager in;

        IndexVersionManagerWrapper(IndexVersionManager in) {
            this.in = in;
        }

        @Override
        public VersionIndexTypeBean topVersionIndex(String indexName) {
            VersionIndexTypeBean bean = in.topVersionIndex(indexName);
            if (bean == null && defaultIndexVersionManagerEnable) {
                bean = defaultIndexVersionManager.topVersionIndex(indexName);
            }
            return bean;
        }

        @Override
        public VersionIndex createNewIndex(String indexName) {
            VersionIndex versionIndex = in.createNewIndex(indexName);
            if (versionIndex == null && defaultIndexVersionManagerEnable) {
                versionIndex = defaultIndexVersionManager.createNewIndex(indexName);
            }
            return versionIndex;
        }

        @Override
        public VersionIndex createNextVersionIndex(VersionIndex curTopVersion) {
            VersionIndex versionIndex;
            if (in.supportMaxVersion() >= curTopVersion.getVersion()) {
                versionIndex = in.createNextVersionIndex(curTopVersion);
            } else {
                versionIndex = null;
            }
            if (versionIndex == null && defaultIndexVersionManagerEnable) {
                versionIndex = defaultIndexVersionManager.createNextVersionIndex(curTopVersion);
            }
            return versionIndex;
        }

        @Override
        public int supportMaxVersion() {
            return in.supportMaxVersion();
        }

        @Override
        public String supportIndex() {
            return in.supportIndex();
        }
    }

}
