const { Kafka } = require("kafkajs");

const consume = async () => {
    const kafka = new Kafka({
        clientId: "1",
        brokers: ["localhost:29092"],
    });
    const consumer = kafka.consumer({ groupId: "test-consumer" });

    await consumer.connect();
    await consumer.subscribe({ topic: "test-streaming-2", fromBeginning: true });
    return consumer;
};

// consume()

module.exports = consume;
