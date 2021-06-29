const express = require('express');
const { Kafka } = require("kafkajs");
const app = express();
const port = 3001;
const fs = require('fs');
const { bufferToStream } = require('./utils');

app.get('/data', async (req, res) => {
    const kafka = new Kafka({
        clientId: "1",
        brokers: ["localhost:29092"],
    });
    const consumer = kafka.consumer({ groupId: "test-consumer-2" });

    await consumer.connect();
    await consumer.subscribe({ topic: "test-streaming-2", fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const fileSize = Buffer.byteLength(message.value);
            const range = req.headers.range;
            if (range && 0) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0], 10);
                const end = parts[1]
                    ? parseInt(parts[1], 10)
                    : fileSize - 1;
                const chunksize = (end - start) + 1
                const file = bufferToStream(message.value, { start, end })
                console.log(file);
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': 'video/mp4',
                }
                res.writeHead(206, head)
                file.pipe(res)
            }
        },
    })
})

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})