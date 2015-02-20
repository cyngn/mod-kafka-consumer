# Mod Kafka Consumer

This module allows you to recieve kafka messages and replay them on to the vert.x event bus on an user specified address.

####To use this module you must have kafka and zookeeper up and running

This is a multi-threaded worker module that consumes kafka messages and then re-broadcast them on an address on the vert.x event bus.

## Name

The module name is `mod-kafka-consumer`

## Configuration

The mod-kafka-consumer module takes the following configuration:

    {
  		"zookeeper.host" : "<zookeeperHost>",
  		"port": "<zookeeperPort>",
  		"workers.per.topic" : <totalWorkersToCreateForEachTopic>,
  		"group.id" : "<kafkaConsumerGroupId>",
  		"backoff.increment.ms" : "<backTimeInMilli>",
  		"autooffset.reset" : "<kafkaAutoOffset>",
  		"topics" : ["<topic1>", "<topic2>"],
  		"eventbus.address" : "<default kafka.message>"
	}
    
For example:

    {
  		"zookeeper.host" : "localhost",
  		"port": "2181",
  		"workers.per.topic" : 3,
  		"group.id" : "testGroup",
  		"backoff.increment.ms" : "100",
  		"autooffset.reset" : "smallest",
  		"topics" : ["testTopic"],
  		"eventbus.address" : "kafka.to.vertx.bridge"
	}

Field breakdown:

* `zookeeper.host` a zookeeper host being used with your kafka clusters
* `port` zookeeper port to connect on
* `workers.per.topic` a thread will be spawned to consume messages up to the total number specified for each topic
* `group.id` the kafka consumer group name that will be consuming related to
* `backoff.increment.ms` backoff interval for contacting broker without messages in milliseconds
* `autooffset.reset` how to reset the offset 
* `topics` the kafka topics to listen for
* `eventbus.address` the vert.x address to publish messages onto when received form kafka

For a deeper look at kafka configuration parameters check [this](http://kafka.apache.org/07/configuration.html) page out.

## Usage

### Deploy the module in your server

```
public void deployKafka(JsonObject config) {
   container.deployModule("com.cyngn.kafka~mod-kafka-consumer~0.5.0", config, deploy -> {
   		if(deploy.failed()) {
        	logger.error("Failed to start kafka consumer module", deploy.cause());
            container.exit();
            return;
        }
        logger.info("kafka consumer module started");
   });
}
```

### Listen for messages

```
	JsonObject config = container.config();
	// add message listener on topic you set in the kafka consumer module                        
    vertx.eventBus().registerHandler(config.getString("eventbus.address"), new Handler<Message>() {
    	@Override
        public void handle(Message message) {
        	// message handling code
        }
    });
```

### Test setup

* cd [yourKafkaInstallDir]
* bin/zookeeper-server-start.sh config/zookeeper.properties
* bin/kafka-server-start.sh config/server.properties
* bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic [yourTestTopic]
* bin/kafka-console-producer.sh --broker-list localhost:9092 --topic [yourTestTopic]
* start your listening app
