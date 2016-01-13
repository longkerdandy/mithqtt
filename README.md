### Mithril-Mqtt
MQTT Message Broker with Scalability written in Java.

[![Build Status](https://travis-ci.org/longkerdandy/mithril-mqtt.svg?branch=master)](https://travis-ci.org/longkerdandy/mithril-mqtt)
[![License](https://img.shields.io/badge/License-Apache%20License%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Join the chat at https://gitter.im/longkerdandy/mithril-mqtt](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/longkerdandy/mithril-mqtt?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is MQTT
[MQTT](http://mqtt.org) is an open industry standard, specifying a light weight publish-subscribe messaging protocol. It is well suited for constrained devices on unreliable networks.

### What is Mithqtt
Mithqtt is an open source, distributed MQTT message broker for real world. It embraces the [Microservices Architecture](http://microservices.io), and designed to fit into complex server-side application.

As a MQTT message broker, Mithqtt scales both horizontally and vertically on commodity hardware to support a large number of concurrent MQTT clients while maintaing low latency and fault tolerence.

As a Microservice, Mithqtt is small self contained with little external dependencies, using pluggable Communicator to coexist with other microservices.

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
