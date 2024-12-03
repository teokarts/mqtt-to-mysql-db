// Import required MySQL promise-based client
const mysql = require('mysql2/promise');

// Define status mappings for different operational states
const STATUS_MAPPINGS = {
  'State Status': {
    0: 'Preparation in progress',
    1: 'Preparation completed',
    2: 'Filling in progress',
    3: 'Filling completed',
    4: 'Digestion in progress',
    5: 'Digestion completed',
    6: 'Digestion completed -  msg waiting confirmation from operator',
    7: 'Digestion completed -  msg add basket and press unload button',
    8: 'Service mode',
  },
  'Machine Status': {
    0: 'Safety lock ready',
    1: 'Emergency',
    2: 'Safety relay OK',
    3: 'Door up locked',
    4: 'Unload door closed',
    5: 'Basket present',
    6: 'Machine ready',
    7: 'Service mode',
  },
  'Mixer Status': {
    1: 'Running FWD',
    2: 'Running REV',
    4: 'OL',
    6: 'Fault',
  },
  'Shredder Status': {
    1: 'Running FWD',
    2: 'Running REV',
    4: 'OL',
    6: 'Fault',
  },
};

// Define alarm mappings based on the provided CSV file
const ALARM_MAPPINGS = {
  0: 'Emergency Stop',
  1: 'Safety Relay',
  2: 'Mixer Error',
  3: 'Mixer Overload',
  4: 'Shredder Error',
  5: 'Shredder Overload',
  8: 'Mass Temperature Sensor Error',
  10: 'Resistance Temperature Sensor Error',
  12: 'Mixer Comm Error',
  13: 'Shredder Comm Error',
  14: 'Scale Comm Error',
  15: 'Energy Meter Comm Error',
};

class StatusTracker {
  constructor(dbConfig) {
    this.dbConfig = dbConfig;
    this.pool = mysql.createPool(dbConfig);

    // Initialize state tracking for various components
    this.previousStates = {
      V1: null, // State Status
      V2: null, // Machine Status
      V3: null, // Mixer Status
      V4: null, // Shredder Status
      V7: null, // Alarm Status
    };

    // Initialize table with rollover check
    this.initializeWithRolloverCheck();
  }

  /**
   * Check table size and perform rollover if needed
   */
  async checkAndRolloverTable() {
    try {
      // Get current record count
      const [rows] = await this.pool.query(
        'SELECT COUNT(*) as count FROM TH001_EVENTS'
      );
      const recordCount = rows[0].count;

      console.log(`Current event record count: ${recordCount}`);

      // If we've reached 200 records, clear the table
      if (recordCount >= 200) {
        console.log('Reaching 200 records, performing rollover...');
        await this.pool.query('TRUNCATE TABLE TH001_EVENTS');
        console.log('Table cleared for rollover');

        // Reset previous states after rollover
        this.previousStates = {
          V1: null,
          V2: null,
          V3: null,
          V4: null,
          V7: null,
        };
      }
    } catch (error) {
      console.error('Error checking/performing rollover:', error);
    }
  }

  /**
   * Initialize the tracker with a rollover check
   */
  async initializeWithRolloverCheck() {
    try {
      await this.checkAndRolloverTable();
    } catch (error) {
      console.error('Error during initialization:', error);
    }
  }

  /**
   * Convert a number to an array of active bit positions
   */
  getBitArray(value) {
    if (value === null || value === undefined) return [];
    const binary = Number(value).toString(2).padStart(16, '0');
    return [...binary].reverse();
  }

  /**
   * Get the status type for a given field
   */
  getStatusType(field) {
    const mapping = {
      V1: 'State Status',
      V2: 'Machine Status',
      V3: 'Mixer Status',
      V4: 'Shredder Status',
      V7: 'Alarm',
    };
    return mapping[field];
  }

  /**
   * Insert event into database with topic
   */
  async logEvent(timestamp, eventType, description, topic) {
    await this.checkAndRolloverTable();

    const sql = `
      INSERT INTO TH001_EVENTS (timestamp, topic, event_type, description)
      VALUES (?, ?, ?, ?)
    `;

    try {
      await this.pool.query(sql, [timestamp, topic, eventType, description]);
      console.log(
        `Event logged: ${eventType} - ${description} for topic ${topic}`
      );
    } catch (error) {
      console.error('Error logging event:', error);
    }
  }

  /**
   * Parse the raw hardware message into a structured format
   */
  parseHardwareMessage(messageStr) {
    // Extract the data portion after the identifier
    const startIndex = messageStr.indexOf('FR":') + 4;
    const dataContent = messageStr.slice(startIndex);

    // Split into key-value pairs and convert to object
    const pairs = dataContent.split(',');
    const result = {};

    // Process each pair into the result object
    pairs.forEach((pair) => {
      if (pair.includes('V33:')) {
        // Special handling for V33 which contains a quoted string
        const match = pair.match(/V33:"([^"]+)"/);
        if (match) {
          result.V33 = match[1];
        }
      } else {
        // Handle normal numeric values
        const [key, value] = pair.split(':');
        if (key && value !== undefined) {
          result[key] = Number(value);
        }
      }
    });

    // Extract topic name from the beginning of the message
    const topic = messageStr.slice(2, messageStr.indexOf('":'));

    return {
      topic: topic,
      data: result,
    };
  }

  /**
   * Process alarm changes
   */
  async processAlarmChanges(currentValue, previousValue, timestamp, topic) {
    if (currentValue === undefined || currentValue === null) return;

    const currentBits = this.getBitArray(currentValue);
    const previousBits =
      previousValue !== null
        ? this.getBitArray(previousValue)
        : Array(16).fill('0');

    // Check each bit position that has a mapping
    for (let bitPos = 0; bitPos < currentBits.length; bitPos++) {
      if (!ALARM_MAPPINGS[bitPos]) continue;

      const currentBitState = currentBits[bitPos] === '1';
      const previousBitState = previousBits[bitPos] === '1';

      if (currentBitState !== previousBitState) {
        const description = `${ALARM_MAPPINGS[bitPos]} - ${
          currentBitState ? 'Activated' : 'Cleared'
        }`;
        await this.logEvent(timestamp, 'Alarm', description, topic);
      }
    }
  }

  /**
   * Process state changes for a specific field
   */
  async processStateChanges(field, currentValue, timestamp, topic) {
    if (currentValue === undefined || currentValue === null) return;

    const statusType = this.getStatusType(field);
    if (!statusType) return;

    // Handle alarms separately
    if (field === 'V7') {
      await this.processAlarmChanges(
        currentValue,
        this.previousStates[field],
        timestamp,
        topic
      );
      this.previousStates[field] = currentValue;
      return;
    }

    // Process other status changes
    const currentBits = this.getBitArray(currentValue);

    // If this is our first processing since server startup,
    // just store the state without logging
    if (this.previousStates[field] === null) {
      console.log(
        `First message for ${field} since server start - storing state without logging`
      );
      this.previousStates[field] = currentValue;
      return;
    }

    const previousBits = this.getBitArray(this.previousStates[field]);

    // Check each bit position that has a defined status
    for (let bitPos = 0; bitPos < currentBits.length; bitPos++) {
      if (!STATUS_MAPPINGS[statusType][bitPos]) continue;

      const currentBitState = currentBits[bitPos] === '1';
      const previousBitState = previousBits[bitPos] === '1';

      if (currentBitState !== previousBitState) {
        const description = `${STATUS_MAPPINGS[statusType][bitPos]} - ${
          currentBitState ? 'Start' : 'Stop'
        }`;
        await this.logEvent(timestamp, statusType, description, topic);
      }
    }

    this.previousStates[field] = currentValue;
  }

  /**
   * Process incoming MQTT message
   */
  async processMessage(message) {
    try {
      // Convert buffer to string and parse the hardware message
      const messageStr = message.toString();
      console.log('Processing raw message:', messageStr);

      const parsedData = this.parseHardwareMessage(messageStr);
      const deviceData = parsedData.data;
      const topic = parsedData.topic;

      const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

      // Process each status field
      await this.processStateChanges('V1', deviceData.V1, timestamp, topic);
      await this.processStateChanges('V2', deviceData.V2, timestamp, topic);
      await this.processStateChanges('V3', deviceData.V3, timestamp, topic);
      await this.processStateChanges('V4', deviceData.V4, timestamp, topic);
      await this.processStateChanges('V7', deviceData.V7, timestamp, topic);
    } catch (error) {
      console.error('Error processing message:', error);
      console.error('Message content:', message.toString());
    }
  }
}

module.exports = StatusTracker;
