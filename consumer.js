const { Kafka } = require("kafkajs");
const fs = require('fs');

const consume = async () => {
    const kafka = new Kafka({
        clientId: "1",
        brokers: ["localhost:29092"],
    });
    const consumer = kafka.consumer({ groupId: "test-consumer" });

    await consumer.connect();
    await consumer.subscribe({ topic: "test-streaming", fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log("Buffer:", message.value);
            fs.writeFile(`./output/${message.offset}.mp4`, message.value, 'binary', function (err) {
                if (err) {
                    console.log(err);
                } else {
                    console.log("Done!");
                }
            });
        },
    })

    return consumer;
};

consume()

module.exports = consume;
