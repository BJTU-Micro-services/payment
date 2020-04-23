import {
  Consumer,
  HighLevelProducer,
  KafkaClient,
  Message,
  ProduceRequest,
} from "kafka-node";
import redisClient from "./db";
import { Order } from "./data";

const kafkaClient = new KafkaClient();
const consumer = new Consumer(kafkaClient, [{ topic: "payment" }, { topic: "new_order" }], {
  autoCommit: false,
});
const producer = new HighLevelProducer(kafkaClient);

consumer.on("message", (message: Message) => {
  if (typeof message.value !== "string") {
    throw new Error("Message type should be 'string'");
  }
  console.log(message);
  try {
    switch (message.topic) {
      case "payment":
        return pay(JSON.parse(message.value));
      case "new_order":
        return createOrder(JSON.parse(message.value));
      default:
        console.error(`Unexpected topic: ${message.topic}`);
    }
  } catch (e) {
    console.error(e);
  }
});

function getOrders(): Promise<Order[]> {
  return new Promise((resolve, reject) =>
    redisClient.get("tickets", (err, reply) =>
      err ? reject(err) : resolve(JSON.parse(reply))
    )
  );
}

function sendPayloads(payloads: ProduceRequest[]): void {
  producer.send(payloads, (err, data) => {
    console.log(err ? err : data);
  });
}

// simulate payment success/failure
const goodCardId = 4242;

async function pay(message: {
  user_id: number;
  card_id: number;
  ticket_id: number;
}) {
  if (message.card_id === goodCardId) {
    const orders = await getOrders();
    const order = orders.find((i) => i.ticket_id === message.ticket_id);

    if (order === undefined) {
      return;
    }

    if (new Date() > order.expiry) {
      return;
    }

    sendPayloads([
      {
        topic: "payment_succesful",
        messages: JSON.stringify({
          user_id: message.user_id,
          ticket_id: message.ticket_id,
        }),
      },
    ]);
  } else {
    sendPayloads([
      {
        topic: "payment_failure",
        messages: JSON.stringify({ user_id: message.user_id }),
      },
    ]);
  }
}

async function createOrder(message: Order) {
  const orders = await getOrders();
  orders.push(message);
  redisClient.set("orders", JSON.stringify(orders));
}
