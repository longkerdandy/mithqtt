package com.github.longkerdandy.mithqtt.communicator.rabbitmq.ex;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

/**
 * Exception Handler for RabbitMQ
 */
public class RabbitMQExceptionHandler extends DefaultExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQExceptionHandler.class);

    @Override
    @SuppressWarnings("StatementWithEmptyBody")
    public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
        // ignore java.net.ConnectException as those are
        // expected during recovery and will only produce noisy
        // traces
        if (exception instanceof ConnectException) {
            // do nothing
        } else {
            logger.error("Caught an exception during connection recovery", exception);
        }
    }

    @Override
    public void handleChannelRecoveryException(Channel ch, Throwable exception) {
        logger.error("Caught an exception when recovering channel {}", ch, exception);
    }

    @Override
    public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
        logger.error("Caught an exception when recovering topology", exception);
    }

    @Override
    protected void handleChannelKiller(Channel channel, Throwable exception, String what) {
        logger.error("{}  threw an exception for channel {}", what, channel, exception);
        try {
            channel.close(AMQP.REPLY_SUCCESS, "Closed due to exception from " + what);
        } catch (AlreadyClosedException | TimeoutException ex) {
            // do nothing
        } catch (IOException ioe) {
            logger.error("Failure during close of channel {} after exception", channel, ioe);
            channel.getConnection().abort(AMQP.INTERNAL_ERROR, "Internal error closing channel for " + what);
        }
    }

    @Override
    protected void handleConnectionKiller(Connection connection, Throwable exception, String what) {
        logger.error("{}  threw an exception for connection {}", what, connection, exception);
        try {
            connection.close(AMQP.REPLY_SUCCESS, "Closed due to exception from " + what);
        } catch (AlreadyClosedException ace) {
            // do nothing
        } catch (IOException ioe) {
            logger.error("Failure during close of connection {} after exception", connection, ioe);
            connection.abort(AMQP.INTERNAL_ERROR, "Internal error closing connection for " + what);
        }
    }
}
