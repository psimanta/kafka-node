const express = require('express');
const app = express();
const port = 3001;
const comsumer = require('./consumer');

app.get('/data', async (req, res) => {
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(message)
        },
    })
    res.send("Consumed!")
})

app.listen(port, () => {
    console.log(`Example app listening at http://localhost:${port}`)
})