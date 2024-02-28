require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');

const topicsConfig = {
  'VivusDigester003/Ecu': 'vivus03',
  'Poursalidis/Ecu': 'SSAKIS',
};

// Latest message storage
let latestMessages = {};

// Define the MQTT broker options
const options = {
  port: process.env.MQTT_PORT,
  username: process.env.MQTT_USERNAME,
  password: process.env.MQTT_PASSWORD,
  clientId: `mqttjs_${Math.random().toString(16).substr(2, 8)}`,
  protocol: 'wss',
};

// Connect to the MQTT broker
const client = mqtt.connect(process.env.MQTT_BROKER, options);

client.on('connect', function () {
  console.log('Connected to the MQTT broker');

  Object.keys(topicsConfig).forEach((topic) => {
    client.subscribe(topic, function (err) {
      if (!err) {
        console.log(`Successfully subscribed to ${topic}`);
      } else {
        console.error(`Failed to subscribe to ${topic}:`, err);
      }
    });
  });
});

client.on('message', (receivedTopic, message) => {
  console.log(`Received message on ${receivedTopic}:`, message.toString());
  if (topicsConfig.hasOwnProperty(receivedTopic)) {
    latestMessages[receivedTopic] = message;
  }
});

// Function to insert data into the database using a new connection pool
async function insertData(topic, message) {
  const table = topicsConfig[topic];
  const data = JSON.parse(message.toString());
  let sql = `INSERT INTO \`${table}\` SET ?`;
  let dataObj = { Topic: topic };
  data.d.forEach((item) => {
    dataObj[item.tag] = item.value;
  });
  dataObj.timestamp = new Date(data.ts)
    .toISOString()
    .slice(0, 19)
    .replace('T', ' ');

  try {
    const pool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });
    const connection = await pool.getConnection();
    await connection.query(sql, dataObj);
    console.log('Data successfully inserted into the database');
    connection.release();
  } catch (error) {
    console.error('Failed to insert data into the database:', error);
  }
}

// Insert the most recent message every 30 minutes
setInterval(() => {
  Object.keys(latestMessages).forEach((topic) => {
    if (latestMessages[topic]) {
      insertData(topic, latestMessages[topic]);
      latestMessages[topic] = null; // Clear after insertion
    }
  });
}, 30 * 60 * 1000); // 30 minutes

// Set up Express.js server
const app = express();

// Serve static files from the current directory
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname + '/index.html'));
});

app.listen(8080, () => {
  console.log('Server is running on port 8080');
});
