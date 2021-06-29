const { Duplex } = require('stream')
const fs = require("fs");
const path = require("path");
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
const ffmpeg = require('fluent-ffmpeg')
ffmpeg.setFfmpegPath(ffmpegPath)

const getTime = (second) => {
    return String(new Date(second * 1000).toISOString().substr(11, 8));
}

module.exports = {
    bufferToStream: (myBuffer) => {
        let tmp = new Duplex();
        tmp.push(myBuffer);
        tmp.push(null);
        return tmp;
    },
    splitVid: (i, startTime, producer) => {
        return new Promise(function (resolve, reject) {
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
}