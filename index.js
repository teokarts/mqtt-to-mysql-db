require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');

const topicsConfig = {
  'VivusDigester003/Ecu': 'vivus03',
  'Poursalidis/Ecu': 'SSAKIS',
  'VivusTHDigester004/Ecu': 'VIVUSTHERMO',
  'data/tsiap01': 'TSIAPANOU',
  'ICR2431/Ch0': 'KARANIS',
};

// Assuming this is at the top level of your index.js
let statsPerTopic = Object.keys(topicsConfig).reduce((acc, topic) => {
  acc[topic] = {
    messageCount: 0,
    dbInsertCount: 0,
    lastInsertTime: Date.now(),
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

// Latest message storage
let lastMessages = {};

// Function to insert data into the database using a new connection pool
async function insertData(topic, message) {
  const table = topicsConfig[topic];

  if (topic === 'ICR2431/Ch0') {
    // Handle the new format (ICR2431/Ch0) in a separate function
    await handleNewFormatData(message, table);
  } else {
    // Existing logic for handling the old format messages
    const data = JSON.parse(message.toString());
    let sql = `INSERT INTO \`${table}\` SET ?`;
    let dataObj = { Topic: topic };

    // Filter out values starting with "OFFSET_"
    data.d.forEach((item) => {
      if (!item.tag.startsWith('OFFSET_')) {
        dataObj[item.tag] = item.value;
      }
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
      console.log('Data successfully inserted into the database (old format)');
      connection.release();
    } catch (error) {
      console.error(
        'Failed to insert data into the database (old format):',
        error
      );
    }
  }
}

// Function to handle the new JSON format (ICR2431/Ch0)
async function handleNewFormatData(message, table) {
  try {
    const data = JSON.parse(message.toString());
    let timestamp;

    if (data.hasOwnProperty('ts')) {
      // Existing format with timezone (ts: "2024-04-19T00:24:39+0300")
      timestamp = data.ts;
    } else if (data.hasOwnProperty('time')) {
      // New format without timezone (time: "2024-04-19 00:23:58.031")
      // Parse the string and adjust based on your needs
      const timeParts = data.time.split(' ');
      const dateString = timeParts[0];
      const timeString = timeParts[1] || '00:00:00'; // Handle missing time
      timestamp = `${dateString}T${timeString}`; // Assuming format YYYY-MM-DDTHH:mm:ss
    } else {
      throw new Error('Missing timestamp field in new format JSON');
    }

    const value = parseFloat(data.value); // Convert value to a number
    const SH = 6;
    const TL = 4;
    const SL = 0.25;
    const TH = 20;

    // Calculate b2
    const b2 = (SH * TL - SL * TH) / (SH - SL);

    // Calculate a2
    const a2 = (TH - b2) / SH;

    // Calculate value to be inserted
    const valueToBeInserted = Math.round((SH - (value - b2) / a2) * 100) / 100;

    const insertData = {
      topic: data.topic, // Assuming "topic" field exists for ICR2431/Ch0
      timestamp: timestamp, // Assuming "time" field holds the timestamp
      value: valueToBeInserted,
    };

    let sql = `INSERT INTO \`${table}\` SET ?`;

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
    await connection.query(sql, insertData);
    console.log('Data successfully inserted into the database (new format)');
    connection.release();
  } catch (error) {
    console.error(
      'Failed to insert data into the database (new format):',
      error
    );
  }
}

function calculateNextInsertTime(lastInsertTime) {
  return new Date(lastInsertTime + 30 * 60 * 1000).toISOString();
}

// Example usage in a function
function updateStats(topic, message) {
  if (statsPerTopic[topic]) {
    statsPerTopic[topic].messageCount += 1;
    statsPerTopic[topic].lastMessageTime = Date.now(); // Update the last message timestamp
  } else {
    console.error(`No stats entry found for topic: ${topic}`);
  }
}

client.on('message', (receivedTopic, message) => {
  console.log(`Received message on ${receivedTopic}:`, message.toString());
  if (topicsConfig.hasOwnProperty(receivedTopic)) {
    lastMessages[receivedTopic] = message; // Store the most recent message
    if (statsPerTopic[receivedTopic]) {
      statsPerTopic[receivedTopic].messageCount += 1;
      statsPerTopic[receivedTopic].lastMessageTime = Date.now(); // Update the last message time
    } else {
      console.error(`No stats entry found for topic: ${receivedTopic}`);
    }
  }
});

function calculateNextInsertTime(lastInsertTime) {
  const nextInsertTime = new Date(lastInsertTime + 30 * 60 * 1000); // 30 minutes from the last insert
  return nextInsertTime;
}

// In your interval where you insert data
setInterval(() => {
  const currentTime = Date.now();
  Object.keys(lastMessages).forEach((topic) => {
    if (lastMessages[topic]) {
      insertData(topic, lastMessages[topic]);
      statsPerTopic[topic].dbInsertCount += 1;
      statsPerTopic[topic].lastInsertTime = currentTime; // Update last insert time
      lastMessages[topic] = null; // Clear after insertion
    }
  });
}, 30 * 60 * 1000); // 30 minutes

// Define a different threshold for considering a topic as inactive
const INACTIVITY_THRESHOLD = 5 * 60 * 1000; // e.g., 5 minutes

// Interval for checking inactivity
setInterval(() => {
  const currentTime = Date.now();
  Object.keys(statsPerTopic).forEach((topic) => {
    if (
      currentTime - statsPerTopic[topic].lastMessageTime >
      INACTIVITY_THRESHOLD
    ) {
      // Mark the topic as inactive
      statsPerTopic[topic].active = false;
    } else {
      // Ensure the topic is marked as active
      statsPerTopic[topic].active = true;
    }
  });
}, INACTIVITY_THRESHOLD);

// Set up Express.js server
const app = express();

// Serve static files from the current directory
app.use(express.static(__dirname));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname + '/index.html'));
});

app.use(cors());

// Add the MySQL connection pool definition at the top level
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Define the function to format uptime
function formatUptime(uptimeInSeconds) {
  const days = Math.floor(uptimeInSeconds / (24 * 3600));
  const hours = Math.floor((uptimeInSeconds % (24 * 3600)) / 3600);
  const minutes = Math.floor((uptimeInSeconds % 3600) / 60);
  const seconds = uptimeInSeconds % 60;
  return `${days} days ${hours} hours ${minutes} minutes ${seconds} seconds`;
}

// Add a new endpoint to fetch server uptime
app.get('/uptime', async (req, res) => {
  try {
    const [rows, fields] = await pool.query(
      "SELECT TIME_FORMAT(SEC_TO_TIME(variable_value), '%H:%i:%s') AS Uptime FROM performance_schema.global_status WHERE variable_name='Uptime'"
    );
    const uptimeInSeconds = parseInt(
      rows[0].Uptime.split(':').reduce((acc, time) => 60 * acc + +time, 0)
    );
    const formattedUptime = formatUptime(uptimeInSeconds);
    res.json({ uptime: formattedUptime });
  } catch (error) {
    console.error('Failed to fetch server uptime:', error);
    res.status(500).json({ error: 'Failed to fetch server uptime' });
  }
});

app.get('/database-size', async (req, res) => {
  try {
    const [rows, fields] = await pool.query(
      "SELECT ROUND(SUM((DATA_LENGTH + INDEX_LENGTH)) / 1024 / 1024, 2) AS DATABASE_SIZE_MB FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'mqtt'"
    );

    // Extract the database size from the result
    const databaseSize = rows[0].DATABASE_SIZE_MB;

    // Send the database size to the client
    res.json({ databaseSize });
  } catch (error) {
    console.error('Failed to fetch database size:', error);
    res.status(500).json({ error: 'Failed to fetch database size' });
  }
});

app.get('/data', (req, res) => {
  const currentTime = new Date().getTime(); // Get the current time in milliseconds
  const preparedData = Object.keys(topicsConfig).map((topic) => {
    const stats = statsPerTopic[topic];
    const nextInsertTime = calculateNextInsertTime(stats.lastInsertTime);
    const timeRemaining = nextInsertTime - currentTime; // Time remaining in milliseconds
    const active = currentTime - stats.lastMessageTime <= INACTIVITY_THRESHOLD; // Check if the topic is active
    console.log(
      `Topic: ${topic}, Last Message: ${stats.lastMessageTime}, Current Time: ${currentTime}, Active: ${active}`
    );

    return {
      topic: topic,
      tableName: topicsConfig[topic],
      messageCount: stats.messageCount,
      dbInsertCount: stats.dbInsertCount,
      timeRemaining: timeRemaining,
      active: active, // Include the active status
    };
  });

  res.json({ stats: preparedData });
});

app.listen(8080, () => {
  console.log('Server is running on port 8080');
});
