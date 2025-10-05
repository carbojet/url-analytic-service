/**
 * Click Counter Service (Node.js)
 * * This is an asynchronous background worker service.
 * * It consumes click events from the Redis queue and persists them to PostgreSQL 
 * * for later analytics and reporting.
 */
const { Pool } = require('pg');
const Redis = require('ioredis');

// --- 1. Database Configuration and Initialization ---

// PostgreSQL Pool for Durable Storage (Source of Truth)
const pgPool = new Pool({
    user: process.env.POSTGRES_USER || 'postgres',
    host: 'postgres', // Docker Compose service name
    database: process.env.POSTGRES_DB || 'urlshortener',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    port: 5432,
    connectionTimeoutMillis: 5000, 
    idleTimeoutMillis: 30000,
});

// Redis Client for Queue Consumption
// Note: We often use a dedicated client for BLPOP to avoid blocking other Redis operations
const redisQueueClient = new Redis({
    host: 'redis', // Docker Compose service name
    port: 6379,
});

redisQueueClient.on('error', (err) => {
    console.error('[REDIS QUEUE] Connection error:', err.message);
});

// Helper function to wait for a delay
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const CLICK_QUEUE_NAME = 'click_events';
const DB_TIMEOUT_SECONDS = 0; // 0 means block indefinitely until an event arrives

// --- 2. Core Worker Logic ---

/**
 * Inserts a single click event into the click_analytics table.
 * @param {object} event - The parsed click event object.
 */
async function saveClickEvent(event) {
    const pgQuery = `
        INSERT INTO click_analytics (short_key, timestamp, user_ip, user_agent, referrer)
        VALUES ($1, $2, $3, $4, $5);
    `;
    
    // NOTE: For this example, user_ip, user_agent, and referrer are placeholders 
    // since the Node.js HTTP server doesn't provide them easily in the current setup.
    // In a real application, the Redirection service would capture and queue this metadata.
    const userIp = '0.0.0.0'; 
    const userAgent = 'unknown'; 
    const referrer = 'unknown';

    try {
        await pgPool.query(pgQuery, [event.key, event.timestamp, userIp, userAgent, referrer]);
        console.log(`[PG WRITE] Click saved for key: ${event.key} at ${event.timestamp}`);
    } catch (error) {
        console.error(`[PG WRITE ERROR] Failed to save click event for key ${event.key}:`, error.message);
        // Log the error but continue running to process the next event
    }
}

/**
 * The main loop that blocks and consumes events from the Redis queue.
 */
async function processQueue() {
    console.log(`[WORKER] Starting queue processor. Listening to '${CLICK_QUEUE_NAME}'...`);

    while (true) {
        try {
            // BLPOP (Blocking Left Pop) waits until an element is pushed to the list
            // Returns: ['click_events', serialized_event_string] or null on timeout
            const result = await redisQueueClient.blpop(CLICK_QUEUE_NAME, DB_TIMEOUT_SECONDS);

            if (result) {
                const [queueName, serializedEvent] = result;
                
                // 1. Parse the event
                const event = JSON.parse(serializedEvent);
                console.log(`[QUEUE CONSUMED] Processing event from queue '${queueName}' for key: ${event.key}`);
                
                // 2. Save the event to PostgreSQL
                await saveClickEvent(event);
            }
            // Since we use a timeout of 0, 'result' will never be null unless Redis disconnects
        } catch (error) {
            console.error('[QUEUE ERROR] Redis consumption failed. Retrying in 5s...', error.message);
            await sleep(5000); // Wait 5 seconds before attempting BLPOP again
        }
    }
}

// --- 3. Connection Initialization and Worker Start ---

/**
 * Attempts to connect to PostgreSQL with retries to handle container startup delay.
 */
async function attemptPgConnectWithRetry(retries = 10, delay = 2000) {
    for (let i = 0; i < retries; i++) {
        try {
            console.log(`[PG INIT] Attempting connection to PostgreSQL... (Attempts left: ${retries - i})`);
            const client = await pgPool.connect();

            // --- NEW: CREATE TABLE IF NOT EXISTS ---
            console.log("[PG INIT] Ensuring 'click_analytics' table exists...");
            await client.query(`
                CREATE TABLE IF NOT EXISTS click_analytics (
                    id SERIAL PRIMARY KEY,
                    short_key VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    user_ip VARCHAR(45),
                    user_agent TEXT,
                    referrer TEXT
                    -- NOTE: Foreign key constraint is often added on initial DB setup (e.g., init.sql) 
                    -- but for guaranteed functionality here, we only ensure the table structure is present.
                );
            `);
            client.release(); 
            console.log("[PG] Successfully connected to PostgreSQL for Click Persistence.");
            return true;
        } catch (err) {
            if (i < retries - 1) {
                console.warn(`[PG INIT] Connection refused. Retrying in ${delay / 1000}s...`);
                await sleep(delay);
            } else {
                console.error("[PG] Initial database connection failed: Exceeded retry limit.", err.stack);
                return false;
            }
        }
    }
    return false;
}

// Wait for Redis connection, then attempt PG connection, then start the worker.
redisQueueClient.on('connect', async () => {
    console.log('[REDIS QUEUE] Successfully connected to Redis.');
    
    const pgSuccess = await attemptPgConnectWithRetry();

    if (pgSuccess) {
        // Start the worker loop only after both Redis and PG are confirmed connected
        processQueue();
    } else {
        console.error('[FATAL] Cannot start Click Counter Service: PostgreSQL connection failed after all retries. Exiting.');
    }
});
