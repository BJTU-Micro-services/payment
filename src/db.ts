import redis from "redis";

const redisClient = redis.createClient();

redisClient.on("error", (error) => console.error(error));

redisClient.set("tickets", JSON.stringify([]));

export default redisClient;
