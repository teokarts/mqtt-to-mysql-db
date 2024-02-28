require('dotenv').config();
const express = require('express');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');
const path = require('path');

const topicsConfig = {
  'VivusDigester003/Ecu': 'vivus03',
  'Poursalidis/Ecu': 'SSAKIS',
  // Add more topics and their corresponding tables as needed
};

// Initialize message and DB insert counts per topic
let statsPerTopic = Object.keys(topicsConfig).reduce((acc, topic) => {
  acc[topic] = {
    messageCount: 0,
    dbInsertCount: 0,
    lastInsertTime: Date.now() - 30 * 60 * 1000,
  };
  return acc;
}, {});

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

console.log('Attempting to connect to the MQTT broker...');

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

let lastMessage = null;

// Function to insert data into the database using a new connection pool
async function insertData(topic, message) {
  const table = topicsConfig[topic]; // Determine the correct table based on the topic
  const data = JSON.parse(message.toString());
  let sql = `INSERT INTO \`${table}\` SET ?`;
  let dataObj = {
    Topic: topic, // Add the topic name as a column value
  };
  data.d.forEach((item) => {
    dataObj[item.tag] = item.value;
  });
  dataObj.timestamp = new Date(data.ts)
    .toISOString()
    .slice(0, 19)
    .replace('T', ' ');

  let pool;
  try {
    // Create a new connection pool
    pool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    // Get a connection from the pool
    const connection = await pool.getConnection();

    // Execute the query
    await connection.query(sql, dataObj);

    console.log('Data successfully inserted into the database');

    dbInsertCount++;
    lastInsertTime = Date.now();
    lastMessage = null;

    // Release the connection back to the pool
    connection.release();
  } catch (error) {
    console.error('Failed to insert data into the database:', error);
  } finally {
    // Close the pool to release its resources
    if (pool) {
      pool.end();
    }
  }
}

Object.keys(topicsConfig).forEach((topic) => {
  client.subscribe(topic, function (err) {
    if (!err) {
      console.log(`Successfully subscribed to ${topic}`);
    }
  });
});

// Initialize to 8 minutes ago
let lastInsertTime = Date.now() - 30 * 60 * 1000;

// Initialize countdown
let countdown = 30 * 60; // 8 minutes in seconds

// Initialize message and DB insert counts
let messageCount = 0;
let dbInsertCount = 0;

// client.on('connect', function () {
//   client.subscribe(topic, function (err) {
//     if (!err) {
//       console.log('Successfully subscribed to the topic');
//     }
//   });
// });

let lastReceivedMessageTime = Date.now();
let noData = false;

let countdownInterval = setInterval(() => {
  if (countdown > 0 && !noData) {
    countdown--;
    const minutes = Math.floor(countdown / 60);
    const seconds = countdown % 60;
    console.log(
      `Time remaining until next insert: ${minutes} minutes and ${seconds} seconds`
    );

    // Check if it's time to insert data into the database
    if (countdown === 0 && lastMessage) {
      // Reset the countdown
      countdown = 30 * 60;

      // Insert data into the database
      insertData();
    }
  } else {
    clearInterval(countdownInterval);
  }
}, 1000);

client.on('message', function (receivedTopic, message) {
  console.log(`Received message on ${receivedTopic}:`, message.toString());

  if (topicsConfig.hasOwnProperty(receivedTopic)) {
    // Update stats for the received topic
    statsPerTopic[receivedTopic].messageCount += 1;
    lastReceivedMessageTime = Date.now();
    insertData(receivedTopic, message);
  }
});

// Set up Express.js server
const app = express();

// Serve static files from the current directory
app.use(express.static(__dirname));

// Use body-parser middleware to parse JSON requests
// app.use(bodyParser.json());

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname + '/index.html'));
});

setInterval(() => {
  if (Date.now() - lastReceivedMessageTime > 5 * 60 * 1000) {
    // No data if no messages in the last 5 minutes
    noData = true;
  }
}, 5 * 60 * 1000);

// Updated '/data' endpoint to return stats per topic
app.get('/data', (req, res) => {
  const stats = Object.entries(statsPerTopic).map(([topic, details]) => ({
    topic,
    tableName: topicsConfig[topic],
    ...details,
    nextInsertTime: details.lastInsertTime + 30 * 60 * 1000,
  }));

  res.json({ stats });
});

app.listen(8080, () => {
  console.log('Server is running on port 8080');
});
