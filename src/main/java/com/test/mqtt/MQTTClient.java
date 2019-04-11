package com.test.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.mqtt.MqttClient;

public class MQTTClient extends AbstractVerticle{

    @Override
    public void start(Future<Void> startFuture) {
        MqttClient client = MqttClient.create(vertx);

        client.connect(1883, "localhost", handler -> {
            if (handler.succeeded()){
                System.out.println("client connected!!");
                vertx.setPeriodic(1000, x -> {
                    client.publish("temperature", Buffer.buffer("hello"), MqttQoS.AT_LEAST_ONCE, false, false);
                });
            }else{
                System.out.println("client unable to connect!!");
            }
        });
    }

    public static void main(String[] args) {
        final Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(MQTTClient.class.getName(), handler -> {
            if (handler.succeeded()){
                System.out.println("MQTT client successfully");
            }else{
                handler.cause().printStackTrace();
            }
        });
    }
}
