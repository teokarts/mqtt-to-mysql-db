require("dotenv").config();
const express = require("express");
const mysql = require("mysql2/promise");
const mqtt = require("mqtt");
const path = require("path");
const bodyParser = require("body-parser");

// Function to insert data into the database using a new connection pool
async function insertData() {
	const data = JSON.parse(lastMessage.toString());
	let sql = "INSERT INTO `TSIAPANOU` SET ?";
	let dataObj = {};
	data.d.forEach((item) => {
		dataObj[item.tag] = item.value;
	});
	dataObj.timestamp = new Date(data.ts)
		.toISOString()
		.slice(0, 19)
		.replace("T", " ");

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

		console.log("Data successfully inserted into the database");

		dbInsertCount++;
		lastInsertTime = Date.now();
		lastMessage = null;

		// Release the connection back to the pool
		connection.release();
	} catch (error) {
		console.error("Failed to insert data into the database:", error);
	} finally {
		// Close the pool to release its resources
		if (pool) {
			pool.end();
		}
	}
}

// Define the MQTT broker options
const options = {
	port: process.env.MQTT_PORT,
	username: process.env.MQTT_USERNAME,
	password: process.env.MQTT_PASSWORD,
	clientId: `mqttjs_${Math.random().toString(16).substr(2, 8)}`,
	protocol: "wss",
};

// Define the MQTT broker address and topic
const broker = process.env.MQTT_BROKER;
const topic = process.env.MQTT_TOPIC;

// Connect to the MQTT broker
const client = mqtt.connect(broker, options);

// Initialize to 8 minutes ago
let lastInsertTime = Date.now() - 10 * 60 * 1000;

// Initialize the last received message
let lastMessage = null;

// Initialize countdown
let countdown = 10 * 60; // 8 minutes in seconds

// Initialize message and DB insert counts
let messageCount = 0;
let dbInsertCount = 0;

client.on("connect", function () {
	client.subscribe(topic, function (err) {
		if (!err) {
			console.log("Successfully subscribed to the topic");
		}
	});
});

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
			countdown = 10 * 60;

			// Insert data into the database
			insertData();
		}
	} else {
		clearInterval(countdownInterval);
	}
}, 1000);

client.on("message", function (topic, message) {
	lastReceivedMessageTime = Date.now();
	noData = false;

	messageCount++;
	lastMessage = message;

	console.log("Received message:", message.toString());
});

// Set up Express.js server
const app = express();

// Serve static files from the current directory
app.use(express.static(__dirname));

// Use body-parser middleware to parse JSON requests
app.use(bodyParser.json());

app.get("/", (req, res) => {
	res.sendFile(path.join(__dirname + "/index.html"));
});

setInterval(() => {
	if (Date.now() - lastReceivedMessageTime > 5 * 60 * 1000) {
		// No data if no messages in the last 5 minutes
		noData = true;
	}
}, 5 * 60 * 1000);

app.get("/data", (req, res) => {
	res.json({
		messageCount,
		dbInsertCount,
		tableName: "TSIAPANOU",
		mqttTopic: topic,
		nextInsertTime: lastInsertTime + 10 * 60 * 1000, // Send the time of the next insert operation
		noData,
	});
});

// Use body-parser middleware to parse JSON requests
app.use(bodyParser.json());

// Define a route to handle table creation requests
app.post("/createTable", (req, res) => {
	// Extract table name and JSON string from the request body
	const { table, jsonString } = req.body;

	try {
		// Parse the JSON string into a JavaScript object
		const jsonData = JSON.parse(jsonString);

		// Extract data array from JSON object
		const data = jsonData.d;

		// Extract tags and their corresponding value types from the data array
		const columns = data.map((item) => {
			const value = item.value;
			const valueType = typeof value;
			let dataType;
			switch (valueType) {
				case "string":
					dataType = "VARCHAR(255)";
					break;
				case "number":
					if (Number.isInteger(value)) {
						dataType = "INT";
					} else {
						dataType = "DECIMAL(10, 2)";
					}
					break;
				case "boolean":
					dataType = "BOOLEAN";
					break;
				default:
					dataType = "VARCHAR(255)";
			}
			return { tag: item.tag, dataType: dataType };
		});

		// Define the SQL query to create the table
		let createTableQuery = `CREATE TABLE IF NOT EXISTS ${table} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME`;

		// Loop through the columns and add them as columns to the table
		columns.forEach((column) => {
			createTableQuery += `,
                ${column.tag} ${column.dataType}`;
		});

		// Close the SQL query
		createTableQuery += ")";

		// Execute the SQL query
		connection.query(createTableQuery, (error, results, fields) => {
			if (error) {
				// If an error occurs, send an error response
				console.error("Failed to create table:", error);
				res
					.status(500)
					.json({ success: false, message: "Failed to create table" });
			} else {
				// If the table is created successfully, send a success response
				console.log("Table created successfully");
				res
					.status(200)
					.json({ success: true, message: "Table created successfully" });
			}
		});
	} catch (error) {
		// If an error occurs during JSON parsing, send an error response
		console.error("Failed to parse JSON string:", error);
		res.status(400).json({ success: false, message: "Invalid JSON string" });
	}
});

//CREATE TABLE END

app.listen(8080, () => {
	console.log("Server is running on port 8080");
});
