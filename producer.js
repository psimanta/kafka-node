const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");

const produce = async () => {
    const kafka = new Kafka({
        clientId: "1",
        brokers: ["localhost:29092"],
        requestTimeout: 25000,
        connectionTimeout: 3000,
    });

    const producer = kafka.producer();

    try {
        await producer.connect();
    } catch (error) {
        console.log("Error:.........", error);
    }


    let movie_data;
    fs.readFile(path.resolve("video.mp4"), async function (err, data) {
        if (err) {
            throw err;
        }
        movie_data = data;

        var i,
            j,
            temparray,
            chunk = 1000000;
        var index = 0;
        for (i = 0, j = movie_data.length; i < j; i += chunk) {
            temparray = movie_data.slice(i, i + chunk);
            //console.log(temparray);
            producer.send({
                topic: "test-streaming",
                messages: [
                    {
                        value: temparray,
                        key: String(index),
                    },
                ],
            });
            index++;
        }
        await producer.disconnect();
    });
};

produce()

module.exports = produce;