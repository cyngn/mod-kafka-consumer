package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import org.junit.Ignore;
import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;
import org.vertx.testtools.VertxAssert;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

/**
 * Basic test show how to consume messages from kafka and then publish them on the event bus.
 *
 * To run test you need to do the following
 *
 * 1) start zookeeper, ie from a kafka local install dir
 *      run: 'bin/zookeeper-server-start.sh  config/zookeeper.properties'
 * 2) start a message broker, ie from a kafka install dir
 *      run: 'bin/kafka-server-start.sh config/server.properties'
 * 3) setup a topic in kafka, ie from the kafka install dir
 *       run: 'bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic testTopic'
 * 4) start the test (note it will time out and fail if it doesn't receive a message in 20 seconds)
 * 5) publish a message on the topic from the kafka CLI, ie from the kafka install dir
 *       run: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic testTopic
 *       run: [type text and press enter]
 *
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 2/19/15
 */
public class MessageConsumerTest extends TestVerticle {

    @Override
    public void start(){
        JsonObject config = new JsonObject();
        config.putString(ConfigConstants.GROUP_ID, "testGroup");
        config.putArray("topics", new JsonArray(new Object [] {"testTopic"}));

        container.deployModule(System.getProperty("vertx.modulename"), config, new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                assertTrue(asyncResult.succeeded());
                assertNotNull("DeploymentID should not be null", asyncResult.result());
                MessageConsumerTest.super.start();
            }
        });
    }

    @Ignore("This is an integration test comment out to actually run it")
    @Test
    public void testMessageReceipt() {
        long timerId = vertx.setTimer(20000, theTimerId ->
        {
            container.logger().info("Failed to get any messages");
            VertxAssert.fail("Test did not complete in 20 seconds");
            container.exit();
        });

        container.logger().info("Registering listener on event bus for kafka messages");

        vertx.eventBus().registerHandler(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS, new Handler<Message>() {
            @Override
            public void handle(Message message) {
                assertTrue(message.body().toString().length() > 0);
                container.logger().info("got message: " + message.body());
                vertx.cancelTimer(timerId);
                testComplete();
                container.exit();
            }
        });
    }
}
