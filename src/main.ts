import {StreetsService, Street, cities, city} from './israeliStreets';
import { connect, Channel } from 'amqplib';
import { MongoClient, Db } from 'mongodb';

const DEFAULT_QUEUE = 'q1';
const DEFAULT_DATABASE = 'streets';
const DEFAULT_DB_URL = 'mongodb://localhost:27017';
const DEFAULT_PUB_PORT = '5672';

let city: city | undefined = undefined;
if (process.argv.length >= 3) {
    const input_city = process.argv[2] as city;
    if (!(input_city in cities)){
        console.error('Missing city');
        process.exit(1);
    }
    city = input_city;
}

if (!city) {
    console.error('Missing input');
    process.exit(1);
}

// Connect to MongoDB
let db: Db;
(async () => {
    try {
        const client = await MongoClient.connect(DEFAULT_DB_URL);
        db = client.db(DEFAULT_DATABASE);
        console.log(`Connected to MongoDB ${DEFAULT_DATABASE}`);
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        process.exit(1);
    }
})();

// Connect to RabbitMQ
let channel: Channel;
(async () => {
    try {
        const conn = await connect(`amqp://guest:guest@localhost:${DEFAULT_PUB_PORT}`);
        channel = await conn.createChannel();
        await channel.assertQueue(DEFAULT_QUEUE);
        console.log(`Connected to RabbitMQ queue ${DEFAULT_QUEUE}`);
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
        process.exit(1);
    }
    // Handle message from RabbitMQ
    const handleMessage = async (msg: any) => {
        const street: Street = JSON.parse(msg.content.toString());
        try {
            // Insert street into MongoDB
            await db.collection('streets').insertOne(street);
            console.log(`Inserted ${street.street_name} in ${street.city_name}`);
            channel.ack(msg); // Acknowledge message
        } catch (error) {
            console.error(error);
            channel.nack(msg); // Reject message and put it back in the queue
        }
    };
    // Consume messages from RabbitMQ
    await channel.consume(DEFAULT_QUEUE, handleMessage);
})();

// Publish streets to RabbitMQ
(async () => {
    const streets = await StreetsService.getStreetsInCity(city);
    for (const street of streets.streets) {
        const streetInfo = await StreetsService.getStreetInfoById(street.streetId);
        const message = JSON.stringify({
            streetId: street.streetId,
            region_code: streetInfo.region_code,
            region_name: streetInfo.region_name,
            city_code: streetInfo.city_code,
            city_name: streetInfo.city_name,
            street_code: streetInfo.street_code,
            street_name: streetInfo.street_name,
            street_name_status: streetInfo.street_name_status,
            official_code: streetInfo.official_code,
        });
        channel.sendToQueue(DEFAULT_QUEUE, Buffer.from(message));
        console.log(`Message sent to queue: ${message}`);
    }
    console.log(`Publish finished`);
})();