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
        indexVersionManager.supportIndex().forEach(index -> {
            String name = CommonUtils.emptyTrim(index);
            if (name == null) return;
            indexVersionManagerMap.put(name, indexVersionManager);
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
        Set<IndexTypeBean> typeBeans;
        if (type == null) {
            typeBeans = configManager.findIndexTypeBean(index);
        } else {
            IndexTypeBean bean = configManager.findIndexTypeBean(new IndexTypeDesc(index, type));
            if (bean != null) {
                typeBeans = new HashSet<>();
                typeBeans.add(bean);
            } else {
                typeBeans = null;
            }
        }

        if (CommonUtils.isEmpty(typeBeans)) return null;

        if (fillingIndex.readOp(list -> list.contains(index))) {
            return null;
        }
        fillingIndex.writeOp(list -> list.add(index));

        VersionIndexTypeBean versionIndex = getVersionIndex(index);
        Map<String, Long> typeDocNum = new HashMap<>();
        typeBeans.forEach(typeBean -> {
            if (versionIndex != null) versionIndex.setIndexTypeBean(typeBean);
            PageDocumentIterator docItr = null;
            try {
                docItr = indexDocFactory.indexDocCreate(versionIndex == null ? typeBean : versionIndex);
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
        IndexVersionManager manager = indexVersionManagerMap.get(index);
        if (defaultIndexVersionManagerEnable) manager = defaultIndexVersionManager;
        if (manager != null) return manager.getIndexVersion(index);
        else return null;
    }

    private int indexGenerate(PageDocument document) {
        return bulkIndexGeneratorMap.getOrDefault(document.getBaseInfo().getType(),
                defaultBulkIndexGenerator).bulkInsert(client, document);
    }

}
