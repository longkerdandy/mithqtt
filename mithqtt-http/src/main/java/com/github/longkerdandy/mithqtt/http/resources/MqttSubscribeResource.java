package com.github.longkerdandy.mithqtt.http.resources;

import com.github.longkerdandy.mithqtt.api.auth.Authenticator;
import com.github.longkerdandy.mithqtt.api.cluster.Cluster;
import com.github.longkerdandy.mithqtt.api.message.Message;
import com.github.longkerdandy.mithqtt.api.message.MqttAdditionalHeader;
import com.github.longkerdandy.mithqtt.api.message.MqttSubscribePayloadGranted;
import com.github.longkerdandy.mithqtt.api.message.MqttTopicSubscriptionGranted;
import com.github.longkerdandy.mithqtt.http.entity.ErrorCode;
import com.github.longkerdandy.mithqtt.http.entity.ErrorEntity;
import com.github.longkerdandy.mithqtt.http.entity.ResultEntity;
import com.github.longkerdandy.mithqtt.http.entity.Subscription;
import com.github.longkerdandy.mithqtt.http.exception.AuthorizeException;
import com.github.longkerdandy.mithqtt.http.exception.ValidateException;
import com.github.longkerdandy.mithqtt.http.util.Validator;
import com.github.longkerdandy.mithqtt.storage.redis.sync.RedisSyncStorage;
import com.github.longkerdandy.mithqtt.util.Topics;
import com.sun.security.auth.UserPrincipal;
import io.dropwizard.auth.Auth;
import io.netty.handler.codec.mqtt.*;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.security.PermitAll;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MQTT Subscribe related resource
 */
@Path("/clients/{clientId}/subscribe")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class MqttSubscribeResource extends AbstractResource {

    private static final Logger logger = LoggerFactory.getLogger(MqttSubscribeResource.class);

    public MqttSubscribeResource(String serverId, Validator validator, RedisSyncStorage redis, Cluster cluster, Authenticator authenticator) {
        super(serverId, validator, redis, cluster, authenticator);
    }

    /**
     * Handle MQTT Subscribe Request in RESTful style
     * Granted QoS Levels will send back to client.
     * Retain Messages matched the subscriptions will NOT send back to client.
     */
    @PermitAll
    @POST
    public ResultEntity<List<MqttGrantedQoS>> subscribe(@PathParam("clientId") String clientId, @Auth UserPrincipal user, @QueryParam("protocol") @DefaultValue("4") byte protocol,
                                                        @QueryParam("packetId") @DefaultValue("0") int packetId,
                                                        List<Subscription> subscriptions) {
        String userName = user.getName();
        MqttVersion version = MqttVersion.fromProtocolLevel(protocol);
        List<MqttTopicSubscription> requestSubscriptions = new ArrayList<>();
        List<MqttTopicSubscriptionGranted> grantedSubscriptions = new ArrayList<>();

        // HTTP interface require valid Client Id
        if (!this.validator.isClientIdValid(clientId)) {
            logger.debug("Protocol violation: Client id {} not valid based on configuration", clientId);
            throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
        }

        // Validate Topic Filter based on configuration
        for (Subscription subscription : subscriptions) {
            if (!this.validator.isTopicFilterValid(subscription.getTopic())) {
                logger.debug("Protocol violation: Client {} subscription {} is not valid based on configuration", clientId, subscription.getTopic());
                throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
            }
            MqttQoS requestQos;
            try {
                requestQos = MqttQoS.valueOf(subscription.getQos());
            } catch (IllegalArgumentException e) {
                logger.debug("Protocol violation: Client {} subscription qos {} is not valid", clientId, subscription.getQos());
                throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
            }
            requestSubscriptions.add(new MqttTopicSubscription(subscription.getTopic(), requestQos));
        }

        logger.debug("Message received: Received SUBSCRIBE message from client {} user {}", clientId, userName);

        // Authorize client subscribe using provided Authenticator
        List<MqttGrantedQoS> grantedQosLevels = this.authenticator.authSubscribe(clientId, userName, requestSubscriptions);
        if (subscriptions.size() != grantedQosLevels.size()) {
            logger.warn("Authorization error: SUBSCRIBE message's subscriptions count not equal to granted QoS count");
            throw new AuthorizeException(new ErrorEntity(ErrorCode.UNAUTHORIZED));
        }
        logger.trace("Authorization granted on topic {} as {} for client {}", ArrayUtils.toString(requestSubscriptions), ArrayUtils.toString(grantedQosLevels), clientId);

        for (int i = 0; i < requestSubscriptions.size(); i++) {

            MqttGrantedQoS grantedQoS = grantedQosLevels.get(i);
            String topic = requestSubscriptions.get(i).topic();
            List<String> topicLevels = Topics.sanitize(topic);
            grantedSubscriptions.add(new MqttTopicSubscriptionGranted(topic, grantedQoS));

            // Granted only
            if (grantedQoS != MqttGrantedQoS.NOT_GRANTED) {

                // If a Server receives a SUBSCRIBE Packet containing a Topic Filter that is identical to an existing
                // Subscription’s Topic Filter then it MUST completely replace that existing Subscription with a new
                // Subscription. The Topic Filter in the new Subscription will be identical to that in the previous Subscription,
                // although its maximum QoS value could be different.
                logger.trace("Update subscription: Update client {} subscription with topic {} QoS {}", clientId, topic, grantedQoS);
                this.redis.updateSubscription(clientId, topicLevels, MqttQoS.valueOf(grantedQoS.value()));
            }
        }

        // Pass message to 3rd party application
        Message<MqttPacketIdVariableHeader, MqttSubscribePayloadGranted> msg = new Message<>(
                new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                new MqttAdditionalHeader(version, clientId, userName, null),
                MqttPacketIdVariableHeader.from(packetId),
                new MqttSubscribePayloadGranted(grantedSubscriptions));
        this.cluster.sendToApplication(msg);

        return new ResultEntity<>(grantedQosLevels);
    }

    /**
     * Get client's exist subscriptions
     */
    @PermitAll
    @GET
    public ResultEntity<List<Subscription>> subscribe(@PathParam("clientId") String clientId, @Auth UserPrincipal user) {
        List<Subscription> subscriptions = new ArrayList<>();

        // HTTP interface require valid Client Id
        if (!this.validator.isClientIdValid(clientId)) {
            logger.debug("Protocol violation: Client id {} not valid based on configuration", clientId);
            throw new ValidateException(new ErrorEntity(ErrorCode.INVALID));
        }

        // Read client's subscriptions from storage
        Map<String, MqttQoS> map = this.redis.getClientSubscriptions(clientId);
        map.forEach((topic, qos) -> subscriptions.add(new Subscription(topic, qos.value())));

        return new ResultEntity<>(subscriptions);
    }
}
