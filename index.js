// Import required dependencies
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');
const util = require('util');
const StatusTracker = require('./statusTracker');

// Configuration for multiple MQTT clients
const mqttConfigs = [
  {
    // Primary broker configuration
    options: {
      port: process.env.MQTT_PORT,
      username: process.env.MQTT_USERNAME,
      password: process.env.MQTT_PASSWORD,
      clientId: `mqttjs_primary_${Math.random().toString(16).substr(2, 8)}`,
      protocol: 'wss',
    },
    broker: process.env.MQTT_BROKER,
    topics: {
      'VivusDigester003/Ecu': 'vivus03',
      'Poursalidis/Ecu': 'SSAKIS',
      'VivusTHDigester004/Ecu': 'VIVUSTHERMO',
      'data/tsiap01': 'TSIAPANOU',
      'ICR2431/Ch0': 'KARANIS',
      'sala001/data': 'SALASIDIS',
      'AirBlock001/data': 'AIRBLOCKDEMO',
      'Agrivoltaics001/data': 'AGRIVOLTAICS',
      'HliasEukarpia/data': 'DELTAIOAEF',
    },
  },
  {
    // Secondary broker configuration for T004009240001FR
    options: {
      port: process.env.MQTT_PORT_2,
      username: process.env.MQTT_USERNAME_2,
      password: process.env.MQTT_PASSWORD_2,
      clientId: `mqttjs_secondary_${Math.random().toString(16).substr(2, 8)}`,
      protocol: 'wss',
    },
    broker: process.env.MQTT_BROKER_2,
    topics: {
      'T004009240001FR/data': 'T004001FR',
    },
  },
];

// Initialize the status tracker
const statusTracker = new StatusTracker({
  host: process.env.THERMO_DB_HOST,
  user: process.env.THERMO_DB_USER,
  password: process.env.THERMO_DB_PASSWORD,
  database: process.env.THERMO_DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Initialize statistics tracking for all topics
const statsPerTopic = Object.keys(
  mqttConfigs.reduce((acc, config) => {
    return { ...acc, ...config.topics };
  }, {})
).reduce((acc, topic) => {
  acc[topic] = {
    messageCount: 0,
    dbInsertCount: 0,
    lastInsertTime: Date.now(),
    lastMessageTime: Date.now(),
    active: false,
  };
  return acc;
}, {});

// Initialize MQTT clients array and last messages storage
const mqttClients = [];
let lastMessages = {};

// Data handling functions
async function handleT004009240001FRData(message, table, topic) {
  try {
    // Convert the message Buffer to a string for processing
    const messageStr = message.toString();
    console.log('Received message:', messageStr);

    // Extract the data portion after the identifier 'FR":'
    const dataStart = messageStr.indexOf('FR":') + 4;
    const dataContent = messageStr.slice(dataStart);

    // Remove the V33 portion from the data content before processing
    // This uses a regular expression to remove everything from V33:" to the next comma or end of string
    const numericContent = dataContent.replace(/,?V33:"[^"]*"/, '');

    // Split the remaining content into individual measurements
    const measurements = numericContent.split(',');

    // Get current timestamp in MySQL datetime format
    const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

    // Initialize the data object with timestamp and topic
    const insertData = {
      timestamp: timestamp,
      topic: topic,
    };

    // Process each measurement, converting to numbers where possible
    measurements.forEach((measurement) => {
      const [key, value] = measurement.split(':');
      // Only process if we have both a key and a value
      if (key && value !== undefined) {
        const columnName = key.trim();
        const numericValue = parseFloat(value);
        // Only add the field if it's a valid number and not V33
        if (!isNaN(numericValue) && columnName !== 'V33') {
          insertData[columnName] = numericValue;
        }
      }
    });

    console.log('Final insertData object:', insertData);

    // Create connection pool for THERMO database
    const thermoPool = mysql.createPool({
      host: process.env.THERMO_DB_HOST,
      user: process.env.THERMO_DB_USER,
      password: process.env.THERMO_DB_PASSWORD,
      database: process.env.THERMO_DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    // Get a connection from the pool and execute the insert
    const connection = await thermoPool.getConnection();
    try {
      const sql = `INSERT INTO \`${table}\` SET ?`;
      const [result] = await connection.query(sql, insertData);
      console.log('Data successfully inserted into the THERMO database');
    } finally {
      // Always release the connection back to the pool
      connection.release();
    }
  } catch (error) {
    console.error('Failed to insert data into the THERMO database:', error);
    console.error('Error details:', error.message);
  }
}
async function insertData(topic, message) {
  const table = topicsConfig[topic];

  try {
    if (topic === 'ICR2431/Ch0') {
      await handleNewFormatData(message, table);
    } else if (topic === 'sala001/data') {
      await handleSalasidis001Data(message, table, topic);
    } else if (topic === 'AirBlock001/data') {
      await handleAirBlockData(message, table, topic);
    } else if (topic === 'Agrivoltaics001/data') {
      await handleAgrivoltaicsData(message, topic);
    } else {
      // Standard format handling
      const data = JSON.parse(message.toString());
      let dataObj = { Topic: topic };

      data.d.forEach((item) => {
        if (!item.tag.startsWith('OFFSET_')) {
          dataObj[item.tag] = item.value;
        }
      });

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
      try {
        await connection.query(`INSERT INTO \`${table}\` SET ?`, dataObj);
        console.log('Data successfully inserted (standard format)');
      } finally {
        connection.release();
      }
    }
  } catch (error) {
    console.error(`Failed to insert data for topic ${topic}:`, error);
  }
}

// Function to set up MQTT client
function setupMqttClient(config) {
  const client = mqtt.connect(config.broker, config.options);

  client.on('connect', function () {
    console.log(`Connected to MQTT broker: ${config.broker}`);

    Object.keys(config.topics).forEach((topic) => {
      client.subscribe(topic, function (err) {
        if (!err) {
          console.log(
            `Successfully subscribed to ${topic} on ${config.broker}`
          );
        } else {
          console.error(
            `Failed to subscribe to ${topic} on ${config.broker}:`,
            err
          );
        }
      });
    });
  });

  client.on('error', function (error) {
    console.error(`MQTT client error for broker ${config.broker}:`, error);
  });

  client.on('message', async (receivedTopic, message) => {
    console.log(`Received message on ${receivedTopic} from ${config.broker}`);

    if (receivedTopic === 'T004009240001FR/data') {
      try {
        console.log('Processing message for status tracking...');
        await statusTracker.processMessage(message);
      } catch (error) {
        console.error('Error processing status changes:', error);
      }
    }

    if (config.topics.hasOwnProperty(receivedTopic)) {
      lastMessages[receivedTopic] = message;
      if (statsPerTopic[receivedTopic]) {
        statsPerTopic[receivedTopic].messageCount += 1;
        statsPerTopic[receivedTopic].lastMessageTime = Date.now();
      } else {
        console.error(`No stats entry found for topic: ${receivedTopic}`);
      }
    }
  });

  return client;
}

// Initialize all MQTT clients
mqttConfigs.forEach((config) => {
  const client = setupMqttClient(config);
  mqttClients.push(client);
});

// Combine all topics configuration
const topicsConfig = mqttConfigs.reduce((acc, config) => {
  return { ...acc, ...config.topics };
}, {});

// ΕΚΕΤΑ data storage
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

// KARANIS Μέτρηση Στάθμης
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

// Αρχική κατάσταση για αποθήκευση στον πίνακα των ALARMS
let lastAlarmState = {
  operationStatus: null,
  latestFault: null,
};

// ΣΑΛΑΣΙΔΗΣ data monitoring και alarm logging
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

// Express application setup
const app = express();
app.use(cors());
app.use(express.static(__dirname));

// Create database pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Helper function to format uptime
function formatUptime(uptimeInSeconds) {
  const days = Math.floor(uptimeInSeconds / (24 * 3600));
  const hours = Math.floor((uptimeInSeconds % (24 * 3600)) / 3600);
  const minutes = Math.floor((uptimeInSeconds % 3600) / 60);
  const seconds = Math.floor(uptimeInSeconds % 60);
  return `${days} days ${hours} hours ${minutes} minutes ${seconds} seconds`;
}

// Database size endpoint
app.get('/database-size', async (req, res) => {
  try {
    const [rows] = await pool.query(`
      SELECT ROUND(SUM((DATA_LENGTH + INDEX_LENGTH)) / 1024 / 1024, 2) AS DATABASE_SIZE_MB 
      FROM information_schema.TABLES 
      WHERE TABLE_SCHEMA = '${process.env.DB_NAME}'
    `);

    const databaseSize = rows[0].DATABASE_SIZE_MB;
    res.json({ databaseSize });
  } catch (error) {
    console.error('Failed to fetch database size:', error);
    res.status(500).json({ error: 'Failed to fetch database size' });
  }
});

// Table info endpoint
app.get('/table-info', async (req, res) => {
  try {
    const mqttPool = mysql.createPool({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    const thermoPool = mysql.createPool({
      host: process.env.THERMO_DB_HOST,
      user: process.env.THERMO_DB_USER,
      password: process.env.THERMO_DB_PASSWORD,
      database: 'THERMO',
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    let mqttResults = [];
    let thermoResults = [];

    const [mqttTables] = await mqttPool.query(
      `SHOW TABLES FROM ${process.env.DB_NAME}`
    );
    for (const tableRow of mqttTables) {
      const tableName = tableRow[`Tables_in_${process.env.DB_NAME}`];
      const [tableInfo] = await mqttPool.query(`
        SELECT 
          '${tableName}' as 'Table Name',
          ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)',
          (SELECT COUNT(*) FROM \`${process.env.DB_NAME}\`.\`${tableName}\`) as 'Row Count',
          'mqtt' as 'Database'
        FROM information_schema.TABLES 
        WHERE table_schema = '${process.env.DB_NAME}'
        AND table_name = '${tableName}'
      `);
      if (tableInfo[0]) mqttResults.push(tableInfo[0]);
    }

    const [thermoTables] = await thermoPool.query('SHOW TABLES FROM THERMO');
    for (const tableRow of thermoTables) {
      const tableName = tableRow['Tables_in_THERMO'];
      const [tableInfo] = await thermoPool.query(`
        SELECT 
          '${tableName}' as 'Table Name',
          ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)',
          (SELECT COUNT(*) FROM THERMO.\`${tableName}\`) as 'Row Count',
          'THERMO' as 'Database'
        FROM information_schema.TABLES 
        WHERE table_schema = 'THERMO'
        AND table_name = '${tableName}'
      `);
      if (tableInfo[0]) thermoResults.push(tableInfo[0]);
    }

    const combinedResults = [...mqttResults, ...thermoResults];
    res.json({ tableInfo: [combinedResults] });

    await mqttPool.end();
    await thermoPool.end();
  } catch (error) {
    console.error('Failed to fetch table info:', error);
    res.status(500).json({
      error: 'Failed to fetch table info',
      details: error.message,
    });
  }
});

// Other endpoints (uptime, node-uptime, api/status-events, data)
app.get('/uptime', async (req, res) => {
  try {
    const [rows] = await pool.query("SHOW GLOBAL STATUS LIKE 'Uptime'");
    if (rows?.[0]?.Value) {
      const uptimeSeconds = parseInt(rows[0].Value, 10);
      res.json({
        uptime: formatUptime(uptimeSeconds),
        uptimeSeconds,
      });
    } else {
      throw new Error('Unable to fetch uptime value');
    }
  } catch (error) {
    console.error('Failed to fetch server uptime:', error);
    res.status(500).json({
      error: 'Failed to fetch server uptime',
      details: error.message,
    });
  }
});

// Continuing from the uptime endpoint...

app.get('/node-uptime', (req, res) => {
  const uptimeSeconds = Math.floor(process.uptime());
  res.json({
    uptime: formatUptime(uptimeSeconds),
    uptimeSeconds: uptimeSeconds,
  });
});

app.get('/api/status-events', async (req, res) => {
  try {
    const { startDate, endDate, eventType } = req.query;
    let sql = 'SELECT * FROM TH001_EVENTS WHERE 1=1';
    const params = [];

    if (startDate && endDate) {
      sql += ' AND timestamp BETWEEN ? AND ?';
      params.push(startDate, endDate);
    }

    if (eventType) {
      sql += ' AND event_type = ?';
      params.push(eventType);
    }

    sql += ' ORDER BY timestamp DESC LIMIT 1000';

    const [rows] = await pool.query(sql, params);
    res.json(rows);
  } catch (error) {
    console.error('Error fetching status events:', error);
    res.status(500).json({ error: 'Failed to fetch status events' });
  }
});

app.get('/data', (req, res) => {
  const currentTime = Date.now();
  const preparedData = Object.keys(topicsConfig).map((topic) => {
    const stats = statsPerTopic[topic];
    const nextInsertTime = new Date(stats.lastInsertTime + 30 * 60 * 1000);
    const timeRemaining = nextInsertTime - currentTime;

    return {
      topic: topic,
      tableName: topicsConfig[topic],
      messageCount: stats.messageCount,
      dbInsertCount: stats.dbInsertCount,
      timeRemaining: timeRemaining,
      active: stats.active,
    };
  });

  res.json({ stats: preparedData });
});

// Periodic data insertion (every 30 minutes)
setInterval(() => {
  const currentTime = Date.now();
  Object.keys(lastMessages).forEach((topic) => {
    if (lastMessages[topic]) {
      if (topic === 'T004009240001FR/data') {
        handleT004009240001FRData(
          lastMessages[topic],
          topicsConfig[topic],
          topic
        );
      } else {
        insertData(topic, lastMessages[topic]);
      }
      statsPerTopic[topic].dbInsertCount += 1;
      statsPerTopic[topic].lastInsertTime = currentTime;
      lastMessages[topic] = null;
    }
  });
}, 30 * 60 * 1000);

// Activity monitoring (every 10 seconds)
setInterval(() => {
  const currentTime = Date.now();
  const INACTIVITY_THRESHOLD = 2 * 60 * 1000; // 2 minutes
  Object.keys(statsPerTopic).forEach((topic) => {
    statsPerTopic[topic].active =
      currentTime - statsPerTopic[topic].lastMessageTime <=
      INACTIVITY_THRESHOLD;
  });
}, 10000);

// Server startup
app.listen(8080, () => {
  console.log('Server is running at http://localhost:8080');
});
