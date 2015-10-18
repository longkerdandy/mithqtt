package io.netty.handler.codec.mqtt;

/**
 * Return Code of {@link io.netty.handler.codec.mqtt.MqttSubAckMessage}
 */
public enum MqttSubAckReturnCode {
    SUCCESS_MAX_QOS0(0),
    SUCCESS_MAX_QOS1(1),
    SUCCESS_MAX_QOS2(2),
    FAILURE(0x80);

    private final int value;

    MqttSubAckReturnCode(int value) {
        this.value = value;
    }

    public static MqttSubAckReturnCode valueOf(int value) {
        for (MqttSubAckReturnCode r : values()) {
            if (r.value == value) {
                return r;
            }
        }
        throw new IllegalArgumentException("invalid suback return code: " + value);
    }

    public int value() {
        return value;
    }
}
