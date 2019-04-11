package com.test.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.mqtt.MqttServer;
import io.vertx.reactivex.mqtt.MqttTopicSubscription;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MQTTServer extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) {
        MqttServer mqttServer = MqttServer.create(vertx);

        mqttServer.endpointHandler(endpoint -> {
            System.out.println("MQTT client [" + endpoint.clientIdentifier() + "] request to connect, clean session = " + endpoint.isCleanSession());

            if (endpoint.auth() != null) {
                System.out.println("[username = " + endpoint.auth().getUsername() + ", password = " + endpoint.auth().getPassword() + "]");
            }
            if (endpoint.will() != null) {
                System.out.println("[will topic = " + endpoint.will().getWillTopic() + " msg = " + endpoint.will().getWillMessage() +
                        " QoS = " + endpoint.will().getWillQos() + " isRetain = " + endpoint.will().isWillRetain() + "]");
            }

            System.out.println("[keep alive timeout = " + endpoint.keepAliveTimeSeconds() + "]");

            endpoint.publishHandler(message -> {

                System.out.println("Just received message [" + message.payload().toString(Charset.defaultCharset()) + "] with QoS [" + message.qosLevel() + "]");

                if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
                    endpoint.publishAcknowledge(message.messageId());
                } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
                    endpoint.publishReceived(message.messageId());
                }

            }).publishReleaseHandler(endpoint::publishComplete);


            endpoint.subscribeHandler(subscribe -> {
                List<MqttQoS> grantedQosLevels = new ArrayList<>();
                for (MqttTopicSubscription s: subscribe.topicSubscriptions()) {
                    System.out.println("Subscription for " + s.topicName() + " with QoS " + s.qualityOfService());
                    grantedQosLevels.add(s.qualityOfService());
                }
                endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
            });

            vertx.setPeriodic(1000, v -> {
                endpoint.publish("hello",
                        Buffer.buffer("Hello from the Vert.x MQTT server"),
                        MqttQoS.EXACTLY_ONCE,
                        false,
                        false);
            });

            endpoint.publishAcknowledgeHandler(messageId -> {

                System.out.println("Received ack for message = " +  messageId);

            }).publishReceivedHandler(messageId -> {

                endpoint.publishRelease(messageId);

            }).publishCompletionHandler(messageId -> {

                System.out.println("Received ack for message = " +  messageId);
            });
            // ack the subscriptions request

            // accept connection from the remote client
            endpoint.accept(false);
        }).listen(ar -> {
            if (ar.succeeded()) {
                System.out.println("MQTT server is listening on port " + ar.result().actualPort());
                startFuture.complete();
            } else {

                System.out.println("Error on starting the server");
                ar.cause().printStackTrace();
            }
        });
    }
}
