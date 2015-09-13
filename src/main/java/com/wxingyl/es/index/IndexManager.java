package com.wxingyl.es.index;

import com.wxingyl.es.exception.IndexIllegalArgumentException;
import com.wxingyl.es.index.doc.IndexDocFactory;
import com.wxingyl.es.index.doc.PageDocument;
import com.wxingyl.es.index.doc.PageDocumentIterator;
import com.wxingyl.es.index.generator.BulkIndexGenerate;
import com.wxingyl.es.index.generator.DefaultBulkIndexGenerator;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.RwLock;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/14.
 * index manager
 */
public class IndexManager {

    private Client client;

    private IndexDocFactory indexDocFactory;

    private Map<IndexTypeDesc, BulkIndexGenerate> bulkIndexGeneratorMap = new HashMap<>();

    private BulkIndexGenerate defaultBulkIndexGenerator;

    private RwLock<List<String>> fillingIndex = CommonUtils.createRwLock(new LinkedList<>());

    public IndexManager(Client client, IndexDocFactory indexDocFactory) {
        this.client = client;
        this.indexDocFactory = indexDocFactory;
        defaultBulkIndexGenerator = new DefaultBulkIndexGenerator();
    }

    public void registerBulkIndexGenerate(BulkIndexGenerate bulkIndexGenerate) {
        if (CommonUtils.isEmpty(bulkIndexGenerate.supportType())) {
            throw new IndexIllegalArgumentException("BulkIndexGenerate: " + bulkIndexGenerate + " supportType is empty");
        }
        bulkIndexGenerate.supportType().forEach(v -> bulkIndexGeneratorMap.put(v, bulkIndexGenerate));
    }

    public long indexFill(IndexTypeBean typeBean) {
        final String index = typeBean.getType().getIndex();
        if (fillingIndex.readOp(list -> list.contains(index))) {
            return 0;
        }
        fillingIndex.writeOp(list -> list.add(index));
        PageDocumentIterator docItr = null;
        try {
            docItr = indexDocFactory.indexDocCreate(typeBean);
            docItr.startFillIndex();
            long num = 0;
            while (docItr.hasNext()) {
                num += indexGenerate(docItr.next());
            }
            return num;
        } finally {
            if (docItr != null) docItr.finishFillIndex();
            fillingIndex.writeOp(list -> list.remove(index));
        }
    }

    private int indexGenerate(PageDocument document) {
        return bulkIndexGeneratorMap.getOrDefault(document.getBaseInfo().getType(),
                defaultBulkIndexGenerator).bulkInsert(client, document);
    }

}
