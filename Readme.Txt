Features Implemented :
Implemeted ClusterManager which will be doing part of zookeeper, start stop feature implemented
Register a Topic taking number of partitions for that topic as Input
Register a Producer
Producer distributes the messsage over the topic partitions using round robin algorithm
Register Consumer keeping groupId as mandatory field
Consumer can subscribe to a topic
Impmented kafka consumer group functionality :
When multiple consumers are subscribed to a topic and belong to the same consumer group, each consumer in the group will receive messages from a different subset of the partitions in the topic.
That means every time there is change in consumer-groups/no. of partitions  , there will be rearrangement for partitions to consumer group mappings


Inside generic Helper createPartitions method helps creating partitions for a topic.
Inside generic Helper crearrangePartitionsWithGroups helps achiveing partitions to consumer mapping keeping consumer groups logic in mind.



Asumptions due 1 hour constraint :
1. All the services are deployed on single server
2. Serialize/Deserialize functionality not implemented for producer /consumer
3. MyKafManager will be acting as a orchestrator here
4. USing Producer partitioing logic as Round robin here


Extensibile things :
1. Taking bootstrap servers as input for producer/consumer 
2. Enable Serializer/Deserializer for producer/consumer, also enabling external users to provide their own Serializer/Deserializer.
3. Enabling producers to accept key as input wuith each object, using which we can enable hash based partitioing mechanism for producers
4. Enable external users to provide their own partitioning logic
4. Enabling multiple brokers so we can implement replication and followers logic on the Cluster.




