"use strict";

const kafka = require("kafka-node"),
  Consumer = kafka.Consumer,
  client = new kafka.KafkaClient(),
  consumer = new Consumer(client, [{ topic: "test_topic", partition: 0 }], {
    autoCommit: false,
  });

consumer.on("message", (message) => {
  console.log(message);
});
