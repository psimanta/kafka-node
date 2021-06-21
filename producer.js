const { Kafka } = require("kafkajs");
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
const ffmpeg = require('fluent-ffmpeg')
ffmpeg.setFfmpegPath(ffmpegPath)
const fs = require("fs");
const path = require("path");

const getTime = (second) => {
    return String(new Date(second * 1000).toISOString().substr(11, 8))
}

const splitVid = (i, startTime, producer) => {
    return new Promise(function (resolve, reject) {
        // do some long running async thingâ€¦
        ffmpeg('./assets/video.mp4')
            .setStartTime(getTime(startTime))
            .setDuration('1')
            .output(`./producer_data/${i}.mp4`)
            .on('end', function (err) {
                if (!err) {
                    resolve(`Success: Conversion Done for ${i}`);
                    movie_data = fs.readFileSync(path.resolve(`./producer_data/${i}.mp4`));
                    producer.send({
                        topic: "test-streaming-2",
                        messages: [
                            {
                                value: movie_data,
                                key: String(i),
                            },
                        ],
                    });
                }
            })
            .on('error', function (err) {
                reject(`Error: Conversion Failed for ${i}`);
                console.log("Error!", err);
            })
            .run()
    });
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

    ffmpeg.ffprobe('./assets/video.mp4', async (error, metadata) => {
        const duration = metadata.format.duration;
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