package com.github.longkerdandy.mithril.mqtt.broker.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Metrics Handler based on Message
 */
public class MessageMetricsHandler extends ChannelDuplexHandler {

    protected final String brokerId;
    protected final InfluxDB influxDB;
    protected final String dbName;

    private String clientId;

    public MessageMetricsHandler(String brokerId, InfluxDB influxDB, String dbName) {
        this.brokerId = brokerId;
        this.influxDB = influxDB;
        this.dbName = dbName;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof MqttMessage) {
            MqttMessage mqtt = (MqttMessage) msg;
            if (this.clientId == null && mqtt.fixedHeader().messageType() == MqttMessageType.CONNECT) {
                this.clientId = ((MqttConnectPayload) mqtt.payload()).clientId();
            }
            if (this.clientId != null) {
                Point.Builder builder = Point.measurement("mqtt_client_" + this.clientId)
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("broker", this.brokerId)
                        .tag("direction", "in")
                        .field("count", 1L);
                switch (mqtt.fixedHeader().messageType()) {
                    case CONNECT:
                        builder = builder.tag("type", "connect");
                        break;
                    case PUBLISH:
                        builder = builder.tag("type", "publish");
                        break;
                    case PUBACK:
                        builder = builder.tag("type", "puback");
                        break;
                    case PUBREC:
                        builder = builder.tag("type", "pubrec");
                        break;
                    case PUBREL:
                        builder = builder.tag("type", "pubrel");
                        break;
                    case PUBCOMP:
                        builder = builder.tag("type", "pubcomp");
                        break;
                    case SUBSCRIBE:
                        builder = builder.tag("type", "subscribe");
                        break;
                    case UNSUBSCRIBE:
                        builder = builder.tag("type", "unsubscribe");
                        break;
                    case PINGREQ:
                        builder = builder.tag("type", "pingreq");
                        break;
                    case DISCONNECT:
                        builder = builder.tag("type", "disconnect");
                        break;
                }
                this.influxDB.write(this.dbName, "default", builder.build());
            }
            Point point = Point.measurement("mqtt_broker_" + this.brokerId)
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("direction", "in")
                    .field("count", 1L)
                    .build();
            this.influxDB.write(this.dbName, "default", point);
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof MqttMessage) {
            MqttMessage mqtt = (MqttMessage) msg;
            if (this.clientId != null) {
                Point.Builder builder = Point.measurement("mqtt_client_" + this.clientId)
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .tag("broker", this.brokerId)
                        .tag("direction", "out")
                        .field("count", 1L);
                switch (mqtt.fixedHeader().messageType()) {
                    case CONNACK:
                        builder = builder.tag("type", "connack");
                        break;
                    case PUBLISH:
                        builder = builder.tag("type", "publish");
                        break;
                    case PUBACK:
                        builder = builder.tag("type", "puback");
                        break;
                    case PUBREC:
                        builder = builder.tag("type", "pubrec");
                        break;
                    case PUBREL:
                        builder = builder.tag("type", "pubrel");
                        break;
                    case PUBCOMP:
                        builder = builder.tag("type", "pubcomp");
                        break;
                    case SUBACK:
                        builder = builder.tag("type", "suback");
                        break;
                    case UNSUBACK:
                        builder = builder.tag("type", "unsuback");
                        break;
                    case PINGRESP:
                        builder = builder.tag("type", "pingresp");
                        break;
                }
                this.influxDB.write(this.dbName, "default", builder.build());
            }
            Point point = Point.measurement("mqtt_broker_" + this.brokerId)
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("direction", "out")
                    .field("count", 1L)
                    .build();
            this.influxDB.write(this.dbName, "default", point);
        }
        ctx.write(msg, promise);
    }
}
