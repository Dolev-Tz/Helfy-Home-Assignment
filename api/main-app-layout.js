import express from 'express';
import mysql from 'mysql2/promise';
import { Kafka } from 'kafkajs';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import log4js from 'log4js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const delay = ms => new Promise(r => setTimeout(r, ms));

log4js.configure({
    appenders: { stdout: { type: 'stdout', layout: { type: 'pattern', pattern: '%m' } } },
    categories: { default: { appenders: ['stdout'], level: 'info' } }
});

const logger = log4js.getLogger();

function logJson(level, userId, action, ip) {
    const logEntry = {
        timestamp: new Date().toISOString(),
        level,
        userId: userId || 'GUEST',
        action,
        ip: ip || '127.0.0.1'
    };
    logger[level.toLowerCase()](JSON.stringify(logEntry));
}

async function registerDebeziumConnector() {
    const connector = {
        name: 'inventory-connector',
        config: {
            'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
            'tasks.max': '1',
            'database.hostname': 'mysql',
            'database.port': '3306',
            'database.user': 'mysqluser',
            'database.password': 'mysqlpw',
            'database.server.id': '184054',
            'topic.prefix': 'inventory',
            'database.include.list': 'inventory',
            'schema.history.internal.kafka.bootstrap.servers': 'kafka:9092',
            'schema.history.internal.kafka.topic': 'schema-changes.inventory'
        }
    };

    while (true) {
        try {
            const res = await fetch('http://connect:8083/connectors', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(connector)
            });
            if (res.ok || res.status === 409) {
                logger.info('Debezium connector ready');
                break;
            }
            throw new Error(`status ${res.status}`);
        } catch (err) {
            logger.warn('Error with Debezium');
            await delay(5000);
        }
    }
}

async function init() {
    const app = express();
    app.use(express.json());
    app.use(express.static(path.join(__dirname, 'public')));

    const kafka = new Kafka({
        clientId: 'sre-app',
        brokers: ['kafka:9092'],
        retry: { retries: 10, initialRetryTime: 3000 }
    });

    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: 'cdc-group' });

    while (true) {
        try {
            await producer.connect();
            await consumer.connect();
            logger.info('Connected to Kafka');
            break;
        } catch {
            logger.warn('Error with Kafka');
            await delay(3000);
        }
    }

    await consumer.subscribe({ topic: 'inventory.users' }).catch(() => {
        logger.warn('Waiting for Debezium topic to be created...');
    });

    consumer.run({
        eachMessage: async ({ message }) => {
            const value = JSON.parse(message.value.toString());
            logger.info(JSON.stringify({
                type: 'CDC_DATABASE_CHANGE',
                timestamp: new Date().toISOString(),
                payload: value.payload
            }));
        }
    });

    let con;
    while (true) {
        try {
            con = await mysql.createConnection({
                host: 'mysql',
                user: 'mysqluser',
                password: 'mysqlpw',
                database: 'inventory'
            });
            await con.query('SELECT 1');
            logger.info('Connected to MySQL');
            break;
        } catch {
            logger.warn('Error with MySQL');
            await delay(3000);
        }
    }

    registerDebeziumConnector();

    app.post('/login', async (req, res) => {
        const { username, password } = req.body;
        const userIp = req.ip;

        try {
            const [results] = await con.query(
                'SELECT * FROM users WHERE username = ? AND password = ?',
                [username, password]
            );

            if (results.length === 0) {
                logJson('WARN', null, `Failed login attempt for: ${username}`, userIp);
                return res.status(401).json({ error: 'Invalid credentials' });
            }

            const user = results[0];
            const token = crypto.randomBytes(32).toString('hex');
            await con.query('UPDATE users SET token = ? WHERE id = ?', [token, user.id]);

            logJson('INFO', user.id, 'User logged in', userIp);

            producer.send({
                topic: 'login-events',
                messages: [{ value: JSON.stringify({ userId: user.id, username, timestamp: new Date() }) }]
            }).catch(err => logger.error('Kafka send failed:', err.message));

            return res.json({ message: 'Login successful', token });
        } catch (err) {
            logger.error('Login error:', err.message);
            return res.status(500).json({ error: 'Internal server error' });
        }
    });

    app.get('/profile', async (req, res) => {
        const token = req.headers['authorization'];
        if (!token) return res.status(401).json({ error: 'Missing token' });

        try {
            const [rows] = await con.query(
                'SELECT id, username FROM users WHERE token = ?',
                [token]
            );
            if (rows.length === 0) return res.status(401).json({ error: 'Invalid token' });
            return res.json(rows[0]);
        } catch (err) {
            logger.error('Profile error:', err.message);
            return res.status(500).json({ error: 'Internal server error' });
        }
    });

    app.listen(3000, () => {
        logger.info('Main App Layout initiated. Server running on port 3000');
    });
}

init();
