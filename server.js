const express = require('express');
const { Kafka } = require("kafkajs");
const app = express();
const port = 3001;
const consumer = require('./consumer')();
const fs = require('fs');
const { Duplex } = require('stream')

function bufferToStream(myBuuffer) {
    let tmp = new Duplex();
    tmp.push(myBuuffer);
    tmp.push(null);
    return tmp;
}

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
            // fs.writeFile(`./output/${message.offset}.mp4`, message.value, 'binary', function (err) {
            //     if (err) {
            //         console.log(err);
            //     } else {
            //         console.log("Done!");
            //     }
            // });
            const fileSize = Buffer.byteLength(message.value);
            const range = req.headers.range;
            if (range) {
                const parts = range.replace(/bytes=/, "").split("-")
                const start = parseInt(parts[0], 10);
                const end = parts[1]
                    ? parseInt(parts[1], 10)
                    : fileSize - 1;
                const chunksize = (end - start) + 1
                const file = bufferToStream(message.value, { start, end })
                const head = {
                    'Content-Range': `bytes ${start}-${end}/${fileSize}`,
                    'Accept-Ranges': 'bytes',
                    'Content-Length': chunksize,
                    'Content-Type': 'video/mp4',
                }
                res.writeHead(206, head)
                file.pipe(res)
            }
            // console.log(Buffer.byteLength(message.value));
        },
    })
})

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})