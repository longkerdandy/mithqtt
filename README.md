### Mithqtt
MQTT Message Broker with Scalability written in Java.

[![Build Status](https://travis-ci.org/longkerdandy/mithqtt.svg?branch=master)](https://travis-ci.org/longkerdandy/mithqtt)
[![License](https://img.shields.io/badge/License-Apache%20License%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Join the chat at https://gitter.im/longkerdandy/mithril-mqtt](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/longkerdandy/mithril-mqtt?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is MQTT
[MQTT](http://mqtt.org) is a machine-to-machine (M2M)/"Internet of Things" connectivity protocol. It was designed as an extremely lightweight publish/subscribe messaging transport. It is useful for connections with remote locations where a small code footprint is required and/or network bandwidth is at a premium.

### What is Mithqtt
Mithqtt is an open source, distributed MQTT message broker for real world. It embraces the [Microservices Architecture](http://microservices.io), and designed to fit into complex server-side application.

As a MQTT message broker, Mithqtt scales both horizontally and vertically on commodity hardware to support a large number of concurrent MQTT clients while maintaing low latency and fault tolerence.

As a Microservice, Mithqtt is small self contained with little external dependencies, expose interface through Cluster and HTTP to other microservices.

### Features
- Fully compatible with MQTT v3.1.1 specification.
  - Support QoS 0, QoS 1, QoS 2.
  - Support session state and clean session.
  - Support session keepalive.
  - Support message delivery retry (when connect).
  - Support retain message.
  - Support topic name and topic filter (with wildcards).
  - Strong message ordering for each session.
- Authentication and Authorization on Connect Publish Subscribe.
- Distributed, decentralized, high availability. Eventually consistent on node state.
- [Redis](http://redis.io) based storage (support 2.8 and 3.0).
- [NATS](http://nats.io) based cluster implementation.
- RESTful HTTP interface.

### Architecture
This is the high level architecture design of Mithqtt and its integration with server side applications.

![Mithqtt Architecture](https://github.com/longkerdandy/mithqtt/blob/master/architecture.jpg)

### Interoperability Test
Mithqtt broker is tested against Eclipse Paho's [MQTT Conformance/Interoperability Testing](http://www.eclipse.org/paho/clients/testing/).

1. Basic Test
~~~
$ python client_test.py -z -d -s
hostname localhost port 1883
clean up starting
clean up finished
Basic test starting
Basic test succeeded
Retained message test starting
Retained message test succeeded
This server is not queueing QoS 0 messages for offline clients
Offline message queueing test succeeded
Will message test succeeded
Overlapping subscriptions test starting
This server is publishing one message for all matching overlapping subscriptions, not one for each.
Overlapping subscriptions test succeeded
Keepalive test starting
Keepalive test succeeded
Redelivery on reconnect test starting
Redelivery on reconnect test succeeded
Zero length clientid test starting
Zero length clientid test succeeded
Subscribe failure test starting
Subscribe failure test succeeded
topics test starting
topics test succeeded
test suite succeeded
~~~
