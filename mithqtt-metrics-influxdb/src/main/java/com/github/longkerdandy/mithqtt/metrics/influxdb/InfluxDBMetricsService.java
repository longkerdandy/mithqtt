package com.github.longkerdandy.mithqtt.metrics.influxdb;

import com.github.longkerdandy.mithqtt.api.metrics.MessageDirection;
import com.github.longkerdandy.mithqtt.api.metrics.MetricsService;
import io.netty.handler.codec.mqtt.MqttMessageType;
import org.apache.commons.configuration.AbstractConfiguration;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * InfluxDB based Metrics Service
 */
@SuppressWarnings("unused")
public class InfluxDBMetricsService implements MetricsService {

    protected InfluxDB influxDB;
    protected String dbName;

    @Override
    public void init(AbstractConfiguration config) {
        this.influxDB = InfluxDBFactory.connect(config.getString("influxdb.url"), config.getString("influxdb.username"), config.getString("influxdb.password"));
        this.influxDB.createDatabase(config.getString("influxdb.dbname"));
        this.influxDB.enableBatch(config.getInt("influxdb.actions"), config.getInt("influxdb.durations"), TimeUnit.MILLISECONDS);
        this.dbName = config.getString("influxdb.dbname");
    }

    @Override
    public void destroy() {
    }

    @Override
    public void measurement(String clientId, String brokerId, MessageDirection direction, MqttMessageType type) {
        Point point = Point.measurement("mqtt_client_" + clientId)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("broker", brokerId)
                .tag("direction", direction.toString())
                .tag("type", getMessageTypeName(type))
                .field("count", 1L)
                .build();
        this.influxDB.write(this.dbName, "default", point);
    }

    @Override
    public void measurement(String brokerId, MessageDirection direction, MqttMessageType type) {
        Point point = Point.measurement("mqtt_broker_" + brokerId)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("direction", direction.toString())
                .tag("type", getMessageTypeName(type))
                .field("count", 1L)
                .build();
        this.influxDB.write(this.dbName, "default", point);
    }

    @Override
    public void measurement(String brokerId, MessageDirection direction, long length) {
        Point point = Point.measurement("mqtt_broker_" + brokerId)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("direction", direction.toString())
                .field("length", length)
                .build();
        this.influxDB.write(this.dbName, "default", point);
    }

    protected String getMessageTypeName(MqttMessageType type) {
        switch (type) {
            case CONNECT:
                return "connect";
            case CONNACK:
                return "connack";
            case PUBLISH:
                return "publish";
            case PUBACK:
                return "puback";
            case PUBREC:
                return "pubrec";
            case PUBREL:
                return "pubrel";
            case PUBCOMP:
                return "pubcomp";
            case SUBSCRIBE:
                return "subscribe";
            case SUBACK:
                return "suback";
            case UNSUBSCRIBE:
                return "unsubscribe";
            case UNSUBACK:
                return "unsuback";
            case PINGREQ:
                return "pingreq";
            case PINGRESP:
                return "pingresp";
            case DISCONNECT:
                return "disconnect";
            default:
                return "unknown";
        }
    }
}
