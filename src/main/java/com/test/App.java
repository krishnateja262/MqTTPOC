package com.test;

import com.test.mqtt.MQTTServer;
import io.vertx.core.Vertx;

public class App {
    public static void main( String[] args ) {
        final Vertx vertx = Vertx.vertx();

        vertx.deployVerticle(MQTTServer.class.getName(), handler -> {
            if (handler.succeeded()){
                System.out.println("MQTT deployed successfully");
            }else{
                handler.cause().printStackTrace();
            }
        });
    }
}
