package com.Service;

import com.DAL.Repository;
import com.Entities.Partition;
import com.Entities.Producer;
import com.Entities.Record;
import com.Utils.GenericHelper;

public class ProducerService {

    private Repository repository;
    private GenericHelper genericHelper;

    public ProducerService(Repository repository) {
        this.repository = repository;
        this.genericHelper = new GenericHelper(repository);
    }

    public void registerProducer(String clientId) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (clientId == null || clientId.isEmpty() || repository.isProducerPresent(clientId)) {
            throw new Exception("ClientID nme is either null or topic already present");
        }
        Producer producer = new Producer();
        producer.setId(clientId);
        repository.addProducer(producer);
    }
    public Record send(String topic, String producerId, Object data) throws Exception {
        genericHelper.validateIfClusterRunning();
        if (!repository.isTopicPresent(topic) || !repository.isProducerPresent(producerId)) {
            throw new Exception("Invalid topic or producerId");
        }
        int lastUsedIndex = repository.getLastUsedIndex(topic);
        int partitionIndex = 0;
        int totalPartitions = repository.getTopic(topic).getPartitionCount();
        partitionIndex = (lastUsedIndex + 1) % totalPartitions;
        repository.addUsedIndex(topic, partitionIndex);

        Partition partition = repository.getPartitionArray(topic)[partitionIndex];
        return partition.addRecord(data);

    }
}
