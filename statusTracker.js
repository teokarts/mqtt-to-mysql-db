// statusTracker.js

const mysql = require('mysql2/promise');

// Updated Status descriptions - removed unwanted statuses
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
    // Removed 'Running', 'SU', 'FU'
    1: 'Running FWD',
    2: 'Running REV',
    4: 'OL',
    6: 'Fault',
  },
  'Shredder Status': {
    // Removed 'Running', 'SU', 'FU'
    1: 'Running FWD',
    2: 'Running REV',
    4: 'OL',
    6: 'Fault',
  },
};

class StatusTracker {
  constructor(dbConfig) {
    this.dbConfig = dbConfig;
    this.pool = mysql.createPool(dbConfig);

    // Initialize previous states
    this.previousStates = {
      V1: null,
      V2: null,
      V3: null,
      V4: null,
    };

    // Initialize table with rollover check
    this.initializeWithRolloverCheck();
  }

  /**
   * Check table size and rollover if needed
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

        // Reset previous states to ensure new initial states are logged
        this.previousStates = {
          V1: null,
          V2: null,
          V3: null,
          V4: null,
        };
      }
    } catch (error) {
      console.error('Error checking/performing rollover:', error);
    }
  }

  /**
   * Initialize with rollover check
   */
  async initializeWithRolloverCheck() {
    try {
      await this.checkAndRolloverTable();
    } catch (error) {
      console.error('Error during initialization:', error);
    }
  }

  /**
   * Convert number to array of active bit positions
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
    };
    return mapping[field];
  }

  /**
   * Insert event into database
   */
  async logEvent(timestamp, eventType, description) {
    // Check rollover before logging new event
    await this.checkAndRolloverTable();

    const sql = `
      INSERT INTO TH001_EVENTS (timestamp, event_type, description)
      VALUES (?, ?, ?)
    `;

    try {
      await this.pool.query(sql, [timestamp, eventType, description]);
      console.log(`Event logged: ${eventType} - ${description}`);
    } catch (error) {
      console.error('Error logging event:', error);
    }
  }

  /**
   * Process state changes for a specific field
   */
  // Modify the processStateChanges method in statusTracker.js

  async processStateChanges(field, currentValue, timestamp) {
    if (currentValue === undefined || currentValue === null) return;

    const statusType = this.getStatusType(field);
    if (!statusType) return;

    const currentBits = this.getBitArray(currentValue);

    // If this is our first processing since server startup,
    // just store the state without logging anything
    if (this.previousStates[field] === null) {
      console.log(
        `First message for ${field} since server start - storing state without logging`
      );
      this.previousStates[field] = currentValue;
      return;
    }

    // Process state changes
    const previousBits = this.getBitArray(this.previousStates[field]);

    for (let bitPos = 0; bitPos < currentBits.length; bitPos++) {
      // Skip if this bit position isn't in our mapping
      if (!STATUS_MAPPINGS[statusType][bitPos]) continue;

      const currentBitState = currentBits[bitPos] === '1';
      const previousBitState = previousBits[bitPos] === '1';

      if (currentBitState !== previousBitState) {
        const description = `${STATUS_MAPPINGS[statusType][bitPos]} - ${
          currentBitState ? 'Activated' : 'Deactivated'
        }`;
        await this.logEvent(timestamp, statusType, description);
      }
    }

    // Update previous state
    this.previousStates[field] = currentValue;
  }

  /**
   * Process incoming MQTT message
   */
  async processMessage(message) {
    try {
      const data = JSON.parse(message);
      const deviceData = data.T004009240001FR.split(',').reduce((acc, item) => {
        const [key, value] = item.split(':');
        acc[key.trim()] = Number(value);
        return acc;
      }, {});

      const timestamp = new Date().toISOString().slice(0, 19).replace('T', ' ');

      // Process each status field
      await this.processStateChanges('V1', deviceData.V1, timestamp);
      await this.processStateChanges('V2', deviceData.V2, timestamp);
      await this.processStateChanges('V3', deviceData.V3, timestamp);
      await this.processStateChanges('V4', deviceData.V4, timestamp);
    } catch (error) {
      console.error('Error processing message:', error);
    }
  }
}

module.exports = StatusTracker;
