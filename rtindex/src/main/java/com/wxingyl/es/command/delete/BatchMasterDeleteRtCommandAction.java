package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.IndexTypeInfo;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/22.
 * BatchMasterDeleteRtCommand default action
 */
public class BatchMasterDeleteRtCommandAction extends AbstractMasterDeleteRtCommand implements BatchMasterDeleteRtCommand {

    private final BulkRequestBuilder bulkRequestBuilder;

    private Map<Client, BulkRequestBuilder> clientBulkRequestBuilderMap;

    public BatchMasterDeleteRtCommandAction(IndexTypeInfo.TableInfo tableInfo,  List<String> ids) {
        super(tableInfo);
        final Client client = tableInfo.getIndexManager().getClient();
        bulkRequestBuilder = client.prepareBulk();
        if (ids != null) {
            final String index = tableInfo.getType().getIndex();
            final String type = tableInfo.getType().getType();
            for (String id : ids) {
                bulkRequestBuilder.add(client.prepareDelete(index, type, id));
            }
        }
    }

    @Override
    public boolean isInvalid() {
        return bulkRequestBuilder.request().numberOfActions() == 0;
    }

    @Override
    public void mergeMasterDeleteRtCommand(SingleMasterDeleteRtCommand rtCommand) {
        if (rtCommand.isInvalid()) return;
        DeleteRequestBuilder requestBuilder = rtCommand.getDeleteRequest();
        if (requestBuilder == null) return;
        Client otherClient = rtCommand.getTableInfo().getIndexManager().getClient();
        if (otherClient.equals(tableInfo.getIndexManager().getClient())) {
            bulkRequestBuilder.add(requestBuilder);
        } else {
            if (clientBulkRequestBuilderMap == null) {
                clientBulkRequestBuilderMap = new HashMap<>();
            }
            BulkRequestBuilder bulkRequestBuilder = clientBulkRequestBuilderMap.get(otherClient);
            if (bulkRequestBuilder == null) {
                clientBulkRequestBuilderMap.put(otherClient, bulkRequestBuilder = otherClient.prepareBulk());
            }
            bulkRequestBuilder.add(requestBuilder);
        }
    }

    @Override
    public int deleteDoc() {
        if (isInvalid()) return 0;
        int total = countSucceedItem(bulkRequestBuilder.get());
        if (clientBulkRequestBuilderMap != null) {
            for (BulkRequestBuilder b : clientBulkRequestBuilderMap.values()) {
                total += countSucceedItem(b.get());
            }
        }
        return total;
    }

    private int countSucceedItem(BulkResponse response) {
        int total = response.getItems().length;
        if (response.hasFailures()) {
            for (BulkItemResponse r : response) {
                if (r.isFailed()) total--;
            }
        }
        return total;
    }

}
