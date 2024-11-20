require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');
const util = require('util');

const topicsConfig = {
  'VivusDigester003/Ecu': 'vivus03',
  'Poursalidis/Ecu': 'SSAKIS',
  'VivusTHDigester004/Ecu': 'VIVUSTHERMO',
  'data/tsiap01': 'TSIAPANOU',
  'ICR2431/Ch0': 'KARANIS',
  'sala001/data': 'SALASIDIS',
  'AirBlock001/data': 'AIRBLOCKDEMO',
  'Agrivoltaics001/data': 'AGRIVOLTAICS',
  'T004009240001FR/data': 'T004001FR',
};

async function handleT004009240001FRData(message, table, topic) {
  try {
    console.log('Received message:', message.toString());
    const data = JSON.parse(message.toString());
    console.log('Parsed data:', data);

    const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

    const insertData = {
      topic: topic,
      timestamp: timestamp,
    };

    // Parse the data string into an object
    const dataString = data.T004009240001FR;
    const regex = /Name:\s*([^,]+)\s*,\s*value:\s*\[([^\]]+)\]/g;
    let match;

    while ((match = regex.exec(dataString)) !== null) {
      const key = match[1].trim();
      const value = match[2].trim();

      console.log('Extracted key:', key, 'Value:', value);

      // Special handling for program arrays
      if (key === 'PrepProg' || key === 'FillProg' || key === 'DigestProg') {
        const progArray = value.split(',').map((v) => parseInt(v.trim()));

        // Select specific array position based on program type
        if (key === 'PrepProg' && progArray.length >= 11) {
          insertData[key] = progArray[10]; // 11th element (index 10)
          console.log(
            `Added to insertData: ${key} = ${progArray[10]} (11th element)`
          );
        } else if (
          (key === 'FillProg' || key === 'DigestProg') &&
          progArray.length >= 9
        ) {
          insertData[key] = progArray[8]; // 9th element (index 8)
          console.log(
            `Added to insertData: ${key} = ${progArray[8]} (9th element)`
          );
        }
        continue;
      }

      // Handle all other numeric values
      const numericValue = parseFloat(value);
      if (!isNaN(numericValue)) {
        insertData[key] = numericValue;
        console.log(`Added to insertData: ${key} = ${numericValue}`);
      }
    }

    console.log('Final insertData object:', insertData);

    const sql = `INSERT INTO \`${table}\` SET ?`;

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
    const [result] = await connection.query(sql, insertData);
    console.log('Query result:', result);
    console.log(
      'Data successfully inserted into the database (T004009240001FR format)'
    );
    connection.release();
  } catch (error) {
    console.error(
      'Failed to insert data into the database (T004009240001FR format):',
      error
    );
    console.error('Error details:', error.message);
    if (error.sql) {
      console.error('SQL query:', error.sql);
    }
  }
}

// Assuming this is at the top level of your index.js
let statsPerTopic = Object.keys(topicsConfig).reduce((acc, topic) => {
  acc[topic] = {
    messageCount: 0,
    dbInsertCount: 0,
    lastInsertTime: Date.now(),
    lastMessageTime: Date.now(),
    active: false,
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
  } else if (topic === 'sala001/data') {
    await handleSalasidis001Data(message, table, topic);
  } else if (topic === 'AirBlock001/data') {
    await handleAirBlockData(message, table, topic);
  } else if (topic === 'T004009240001FR/data') {
    await handleT004009240001FRData(message, table, topic);
  } else if (topic === 'Agrivoltaics001/data') {
    await handleAgrivoltaicsData(message, topic);
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

// New function to handle Agrivoltaics data
async function handleAgrivoltaicsData(message, topic) {
  try {
    const data = JSON.parse(message.toString());
    let sql = `INSERT INTO AGRIVOLTAICS SET ?`;
    let dataObj = { topic: topic };

    // Process the data
    data.d.forEach((item) => {
      // Replace colon with underscore in the tag name
      const columnName = item.tag.replace(':', '_');
      dataObj[columnName] = item.value;
    });

    // Add timestamp
    dataObj.timestamp = new Date(data.ts)
      .toISOString()
      .slice(0, 19)
      .replace('T', ' ');

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
    console.log('Data successfully inserted into the AGRIVOLTAICS table');
    connection.release();
  } catch (error) {
    console.error('Failed to insert data into the AGRIVOLTAICS table:', error);
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
    const SL = 0;
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
      transmitter_value: data.value,
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

// Add this variable at the top level of your file, outside of any function
let lastAlarmState = {
  operationStatus: null,
  latestFault: null,
};

async function handleSalasidis001Data(message, table, topic) {
  try {
    console.log('Received message:', message.toString());
    const data = JSON.parse(message.toString());
    console.log('Parsed data:', data);

    const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

    const insertData = {
      topic: topic,
      timestamp: timestamp,
    };

    console.log('Sala001Data:', data.Sala001Data);

    // Parse the Sala001Data string into an object
    const sala001DataString = data.Sala001Data;
    const regex = /Name:\s*([^,]+)\s*,\s*value:\s*\[([^\]]+)\]/g;
    let match;

    while ((match = regex.exec(sala001DataString)) !== null) {
      const key = match[1].trim();
      const value = match[2].trim();

      console.log('Extracted key:', key, 'Value:', value);

      const numericValue = parseFloat(value);

      if (!isNaN(numericValue)) {
        insertData[key] = numericValue;
        console.log(`Added to insertData: ${key} = ${numericValue}`);
      } else {
        console.log(`Skipped invalid numeric value for ${key}`);
      }
    }

    console.log('Final insertData object:', insertData);

    // Generate the SQL query dynamically based on the received fields
    const fields = Object.keys(insertData).join(', ');
    const placeholders = Object.keys(insertData)
      .map(() => '?')
      .join(', ');
    const sql = `INSERT INTO \`${table}\` SET ?`;

    console.log('SQL query:', sql);
    console.log('SQL values:', Object.values(insertData));

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
    const [result] = await connection.query(sql, insertData);
    console.log('Query result:', result);
    console.log(
      'Data successfully inserted into the database (Sala001 format)'
    );

    // Check and insert alarm if necessary
    if (insertData.OperationStatus === 3) {
      if (
        lastAlarmState.operationStatus !== 3 ||
        (lastAlarmState.operationStatus === 3 &&
          lastAlarmState.latestFault !== insertData.LatestFault)
      ) {
        const insertAlarmSql =
          'INSERT INTO SALALARMS (timestamp, LatestFault) VALUES (?, ?)';
        await connection.query(insertAlarmSql, [
          timestamp,
          insertData.LatestFault,
        ]);
        console.log('New alarm inserted into SALALARMS table');

        // Update the last alarm state
        lastAlarmState.operationStatus = insertData.OperationStatus;
        lastAlarmState.latestFault = insertData.LatestFault;
      }
    } else {
      // Reset the last alarm state if OperationStatus is not 3
      lastAlarmState.operationStatus = insertData.OperationStatus;
      lastAlarmState.latestFault = null;
    }

    connection.release();
  } catch (error) {
    console.error(
      'Failed to insert data into the database (Sala001 format):',
      error
    );
    console.error('Error details:', error.message);
    if (error.sql) {
      console.error('SQL query:', error.sql);
    }
  }
}

async function handleAirBlockData(message, table, topic) {
  try {
    console.log('Received message:', message.toString());
    const data = JSON.parse(message.toString());
    console.log('Parsed data:', data);

    const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

    const insertData = {
      topic: topic,
      timestamp: timestamp,
    };

    console.log('Airblock001Data:', data.Airblock001Data);

    // Parse the Airblock001Data string into an object
    const airblockDataString = data.Airblock001Data;
    const regex = /Name:\s*([^,]+)\s*,\s*value:\s*\[([^\]]+)\]/g;
    let match;

    while ((match = regex.exec(airblockDataString)) !== null) {
      const key = match[1].trim();
      const value = match[2].trim();

      console.log('Extracted key:', key, 'Value:', value);

      const numericValue = parseFloat(value);

      if (!isNaN(numericValue)) {
        insertData[key] = numericValue;
        console.log(`Added to insertData: ${key} = ${numericValue}`);
      } else {
        console.log(`Skipped invalid numeric value for ${key}`);
      }
    }

    console.log('Final insertData object:', insertData);

    // Generate the SQL query dynamically based on the received fields
    const fields = Object.keys(insertData).join(', ');
    const placeholders = Object.keys(insertData)
      .map(() => '?')
      .join(', ');
    const sql = `INSERT INTO \`${table}\` (${fields}) VALUES (${placeholders})`;

    console.log('SQL query:', sql);
    console.log('SQL values:', Object.values(insertData));

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
    const [result] = await connection.query(sql, Object.values(insertData));
    console.log('Query result:', result);
    console.log(
      'Data successfully inserted into the database (AirBlock format)'
    );
    connection.release();
  } catch (error) {
    console.error(
      'Failed to insert data into the database (AirBlock format):',
      error
    );
    console.error('Error details:', error.message);
    if (error.sql) {
      console.error('SQL query:', error.sql);
    }
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
  if (topicsConfig.hasOwnProperty(receivedTopic)) {
    // console.log(`Received message on ${receivedTopic}:`, message.toString());
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

function formatUptime(uptimeInSeconds) {
  const days = Math.floor(uptimeInSeconds / (24 * 3600));
  const hours = Math.floor((uptimeInSeconds % (24 * 3600)) / 3600);
  const minutes = Math.floor((uptimeInSeconds % 3600) / 60);
  const seconds = Math.floor(uptimeInSeconds % 60);
  return `${days} days ${hours} hours ${minutes} minutes ${seconds} seconds`;
}

app.get('/uptime', async (req, res) => {
  try {
    const [rows, fields] = await pool.query(
      "SELECT variable_value AS uptime_seconds FROM performance_schema.global_status WHERE variable_name='Uptime'"
    );
    const uptimeInSeconds = parseInt(rows[0].uptime_seconds, 10);
    const formattedUptime = formatUptime(uptimeInSeconds);
    res.json({ uptime: formattedUptime });
  } catch (error) {
    console.error('Failed to fetch server uptime:', error);
    res.status(500).json({ error: 'Failed to fetch server uptime' });
  }
});

function formatUptime(uptimeInSeconds) {
  const days = Math.floor(uptimeInSeconds / (24 * 3600));
  const hours = Math.floor((uptimeInSeconds % (24 * 3600)) / 3600);
  const minutes = Math.floor((uptimeInSeconds % 3600) / 60);
  const seconds = Math.floor(uptimeInSeconds % 60);
  return `${days} days ${hours} hours ${minutes} minutes ${seconds} seconds`;
}
// Add the uptime endpoint
app.get('/node-uptime', (req, res) => {
  const uptimeInSeconds = process.uptime();
  const formattedNodeUptime = formatUptime(uptimeInSeconds);
  res.json({ uptime: formattedNodeUptime });
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

app.get('/table-info', async (req, res) => {
  try {
    const [rows, fields] = await pool.query('CALL GetTableInfo()');
    res.json({ tableInfo: rows });
  } catch (error) {
    console.error('Failed to fetch table info:', error);
    res.status(500).json({ error: 'Failed to fetch table info' });
  }
});

app.listen(8080, () => {
  console.log('Server is running on port 8080');
});
