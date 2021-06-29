const { Kafka } = require("kafkajs");
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
const ffmpeg = require('fluent-ffmpeg')
ffmpeg.setFfmpegPath(ffmpegPath)
const fs = require("fs");
const { splitVid } = require('./utils');

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

    ffmpeg.ffprobe('./assets/video.mp4', async (error, metadata) => {
        const duration = metadata.format.duration;
        fs.writeFileSync("metadata.json", JSON.stringify(metadata));
        for (let i = 0, startTime = 0; startTime <= duration; i++, startTime += 1) {
            var res = await splitVid(i, startTime, producer);
            console.log(res);
        }
        console.log("Splitting Done")
        await producer.disconnect();
    });
}

produce();



module.exports = produce;