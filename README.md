### Mithril-Mqtt
MQTT Message Broker with Scalability.

### Interoperability Test
Mithril-Mqtt broker is tested against Eclipse Paho's [MQTT Conformance/Interoperability Testing](http://www.eclipse.org/paho/clients/testing/).

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
