package com.wxingyl.es.index;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.doc.DefaultIndexDocFactory;
import com.wxingyl.es.index.doc.IndexDocFactory;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.index.doc.PageDocumentIterator;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
import com.wxingyl.es.index.generator.DefaultBulkIndexGenerator;
import com.wxingyl.es.index.version.DefaultIndexVersionManager;
import com.wxingyl.es.index.version.IndexVersionManager;
import com.wxingyl.es.index.version.VersionIndex;
import com.wxingyl.es.index.version.VersionIndexTypeBean;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.RwLock;
import org.elasticsearch.client.Client;

import java.util.*;

/**
 * Created by xing on 15/9/14.
 * index manager
 */
public class IndexManager {

    private Client client;

    private IndexDocFactory indexDocFactory;

    private ConfigManager configManager;

    private BulkIndexGenerate defaultBulkIndexGenerator;

    private Map<IndexTypeDesc, BulkIndexGenerate> bulkIndexGeneratorMap = new HashMap<>();

    private volatile boolean defaultIndexVersionManagerEnable;

    private IndexVersionManager defaultIndexVersionManager;

    private Map<String, IndexVersionManager> indexVersionManagerMap = new HashMap<>();

    private RwLock<List<String>> fillingIndex = CommonUtils.createRwLock(new LinkedList<>());

    public IndexManager(Client client, ConfigManager configManager) {
        this(client, configManager, new DefaultIndexDocFactory());
    }

    public IndexManager(Client client, ConfigManager configManager, IndexDocFactory indexDocFactory) {
        this.client = client;
        this.indexDocFactory = indexDocFactory;
        this.configManager = configManager;
        defaultBulkIndexGenerator = new DefaultBulkIndexGenerator();
    }

    public void enableDefaultIndexVersionManager() {
        defaultIndexVersionManagerEnable = true;
        if (defaultIndexVersionManager == null) {
            defaultIndexVersionManager = new DefaultIndexVersionManager(client);
        }
    }

    public void disableDefaultIndexVersionManager() {
        defaultIndexVersionManagerEnable = false;
    }

    public void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate) {
        if (CommonUtils.isEmpty(bulkIndexGenerate.supportType())) {
            throw new IndexIllegalArgumentException("BulkIndexGenerate: " + bulkIndexGenerate + " supportType is empty");
        }
        bulkIndexGenerate.supportType().forEach(v -> {
            if (v == null) return;
            bulkIndexGeneratorMap.put(v, bulkIndexGenerate);
        });
    }

    public void registerIndexVersionManager(IndexVersionManager indexVersionManager) {
        if (CommonUtils.isEmpty(indexVersionManager.supportIndex())) {
            throw new IndexIllegalArgumentException("IndexVersionManager: " + indexVersionManager + " supportIndex is empty");
        }
        IndexVersionManager manager = new IndexVersionManagerWrapper(indexVersionManager);
        manager.supportIndex().forEach(index -> {
            String name = CommonUtils.emptyTrim(index);
            if (name == null) return;
            indexVersionManagerMap.put(name, manager);
        });
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public IndexDocFactory getIndexDocFactory() {
        return indexDocFactory;
    }

    public long indexFill(IndexTypeDesc type) {
        return indexFill(type.getIndex(), type.getType());
    }

    public long indexFill(String index, String type) {
        Objects.requireNonNull(type);
        Map<String, Long> numMap = innerIndexFill(index, type);
        if (numMap == null) return 0;
        else return numMap.getOrDefault(type, 0l);
    }

    public Map<String, Long> indexFill(String index) {
        return innerIndexFill(index, null);
    }

    private Map<String, Long> innerIndexFill(String index, String type) {
        Set<IndexTypeBean> typeBeanSet;
        if (type == null) {
            typeBeanSet = configManager.findIndexTypeBean(index);
        } else {
            IndexTypeBean bean = configManager.findIndexTypeBean(new IndexTypeDesc(index, type));
            if (bean != null) {
                typeBeanSet = new HashSet<>();
                typeBeanSet.add(bean);
            } else {
                typeBeanSet = null;
            }
        }

        if (CommonUtils.isEmpty(typeBeanSet)) return null;

        if (fillingIndex.readOp(list -> list.contains(index))) {
            return null;
        }
        fillingIndex.writeOp(list -> list.add(index));

        VersionIndexTypeBean topVersionIndex = getVersionIndex(index);
        VersionIndexTypeBean createVersionIndex;
        if (topVersionIndex != null) {
            createVersionIndex = createNextVersionIndex(index, topVersionIndex);
        } else {
            createVersionIndex = null;
        }
        Map<String, Long> typeDocNum = new HashMap<>();
        typeBeanSet.forEach(typeBean -> {
            if (createVersionIndex != null) createVersionIndex.setIndexTypeBean(typeBean);
            PageDocumentIterator docItr = null;
            try {
                docItr = indexDocFactory.indexDocCreate(createVersionIndex == null ? typeBean : createVersionIndex);
                docItr.startFillIndex();
                long num = 0;
                while (docItr.hasNext()) {
                    num += indexGenerate(docItr.next());
                }
                typeDocNum.put(typeBean.getType().getType(), num);
            } finally {
                if (docItr != null) docItr.finishFillIndex();
                fillingIndex.writeOp(list -> list.remove(index));
            }
        });
        return typeDocNum;
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

    private int indexGenerate(PageDocument document) {
        return bulkIndexGeneratorMap.getOrDefault(document.getBaseInfo().getType(),
                defaultBulkIndexGenerator).bulkInsert(client, document);
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
            VersionIndex versionIndex = in.createNextVersionIndex(curTopVersion);
            if (versionIndex == null && defaultIndexVersionManagerEnable) {
                versionIndex = defaultIndexVersionManager.createNextVersionIndex(curTopVersion);
            }
            return versionIndex;
        }

        @Override
        public Set<String> supportIndex() {
            return in.supportIndex();
        }
    }

}
