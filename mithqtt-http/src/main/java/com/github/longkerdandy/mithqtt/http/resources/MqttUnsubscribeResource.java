package com.github.longkerdandy.mithqtt.http.resources;

import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.comm.HttpCommunicator;
import com.github.longkerdandy.mithqtt.api.internal.InternalMessage;
import com.github.longkerdandy.mithqtt.api.internal.Unsubscribe;
import com.github.longkerdandy.mithqtt.api.metrics.MetricsService;
import com.github.longkerdandy.mithqtt.http.entity.ErrorCode;
import com.github.longkerdandy.mithqtt.http.entity.ErrorEntity;
import com.github.longkerdandy.mithqtt.http.entity.ResultEntity;
import com.github.longkerdandy.mithqtt.http.exception.ValidateException;
import com.github.longkerdandy.mithqtt.http.util.Validator;
import com.github.longkerdandy.mithqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithqtt.util.Topics;
import com.sun.security.auth.UserPrincipal;
import io.dropwizard.auth.Auth;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * MQTT Un-Subscribe related resource
 */
@Path("/clients/{clientId}/unsubscribe")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MqttUnsubscribeResource extends AbstractResource {

    private static final Logger logger = LoggerFactory.getLogger(MqttSubscribeResource.class);

    public MqttUnsubscribeResource(String serverId, Validator validator, RedisSyncStorage redis, HttpCommunicator communicator, Authenticator authenticator, MetricsService metrics) {
        super(serverId, validator, redis, communicator, authenticator, metrics);
    }

    @PermitAll
    @POST
    /**
     * Handle MQTT Un-Subscribe Request in RESTful style
     */
    public ResultEntity<Boolean> unsubscribe(@PathParam("clientId") String clientId, @Auth UserPrincipal user, @QueryParam("protocol") @DefaultValue("4") byte protocol,
                                             @QueryParam("packetId") @DefaultValue("0") int packetId,
                                             List<String> topics) {
        String userName = user.getName();
        MqttVersion version = MqttVersion.fromProtocolLevel(protocol);

        // HTTP interface require valid Client Id
        if (!this.validator.isClientIdValid(clientId)) {
            logger.debug("Protocol violation: Client id {} not valid based on configuration", clientId);
            throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
        }

        // Validate Topic Filter based on configuration
        for (String topic : topics) {
            if (!this.validator.isTopicFilterValid(topic)) {
                logger.debug("Protocol violation: Client {} un-subscription {} is not valid based on configuration", clientId, topic);
                throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
            }
        }

        logger.debug("Message received: Received UNSUBSCRIBE message from client {} user {} topics {}", clientId, userName, ArrayUtils.toString(topics));

        // The Topic Filters (whether they contain wildcards or not) supplied in an UNSUBSCRIBE packet MUST be
        // compared character-by-character with the current set of Topic Filters held by the Server for the Client. If
        // any filter matches exactly then its owning Subscription is deleted, otherwise no additional processing
        // occurs
        // If a Server deletes a Subscription:
        // It MUST stop adding any new messages for delivery to the Client.
        //1 It MUST complete the delivery of any QoS 1 or QoS 2 messages which it has started to send to
        // the Client.
        // It MAY continue to deliver any existing messages buffered for delivery to the Client.
        topics.forEach(topic -> {
            logger.trace("Remove subscription: Remove client {} subscription with topic {}", clientId, topic);
            this.redis.removeSubscription(clientId, Topics.sanitize(topic));
        });

        // Pass message to 3rd party application
        Unsubscribe us = new Unsubscribe(packetId, topics);
        InternalMessage<Unsubscribe> m = new InternalMessage<>(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, version, clientId, null, null, us);
        this.communicator.sendToApplication(m);

        return new ResultEntity<>(true);
    }
}
