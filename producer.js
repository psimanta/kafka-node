const { Kafka } = require("kafkajs");
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
const ffmpeg = require('fluent-ffmpeg')
ffmpeg.setFfmpegPath(ffmpegPath)
const fs = require("fs");
const path = require("path");

const getTime = (second) => {
    return String(new Date(second * 1000).toISOString().substr(11, 8))
}

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

    ffmpeg.ffprobe('./assets/video.mp4', (error, metadata) => {
        const duration = metadata.format.duration;
        for (let i = 0, startTime = 0; startTime <= duration; i++, startTime += 3) {
            ffmpeg('./assets/video.mp4')
                .setStartTime(getTime(startTime))
                .setDuration('3')
                .output(`./producer_data/${i}.mp4`)
                .on('end', function (err) {
                    if (!err) { console.log('conversion Done') }
                    movie_data = fs.readFileSync(path.resolve(`./producer_data/${i}.mp4`));
                    producer.send({
                        topic: "test-streaming",
                        messages: [
                            {
                                value: movie_data,
                                key: String(i),
                            },
                        ],
                    });

                })
                .on('error', function (err) {
                    console.log('error: ', err)
                }).run()
        }
    });

    //await producer.disconnect();
}

produce();



module.exports = produce;