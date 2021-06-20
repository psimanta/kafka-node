# Streaming using kafka and NodeJS

 - After Cloning the project create two folders named **producer_data** and **output** in the root directory
 - If you run the **producer.js** file it will read the video data from assets folder and split it and write to kafka topic named **test-streaming**
 - If you run the **consumer.js** file it will read data from kafka topic **test-streaming** and place them in **output** folder
