import {StreetsService, Street, cities, city} from './israeliStreets';
import { connect, Channel } from 'amqplib';
import { MongoClient, Db } from 'mongodb';

const QUEUE_NAME = 'q1';
const DB_NAME = 'streets';
const DB_URL = 'mongodb://localhost:27017';
const PUBLISHER_PORT = '5672';

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

let db: Db;
let channel: Channel;
let finished_publish = false;

async function consumeMessages() {
    // Handle message from RabbitMQ
    const handleMessage = async (msg: any) => {
        const street: Street = JSON.parse(msg.content.toString());
        try {
            // Insert street into MongoDB
            await db.collection('streets').insertOne(street);
            console.log(`Inserted ${street.street_name} in ${street.city_name}`);
            channel.ack(msg);
        } catch (error) {
            console.error(error);
            channel.nack(msg);
        }
        if (finished_publish){
            console.error('Finished Successfully');
            process.exit(1);
        }
    };

    // Consume messages from RabbitMQ
    await channel.consume(QUEUE_NAME, handleMessage);
}

async function publishMessages() {
    // Publish streets to RabbitMQ
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
        channel.sendToQueue(QUEUE_NAME, Buffer.from(message));
        console.log(`Message sent to queue: ${message}`);
    }
    finished_publish=true;
    console.log(`Publish finished`);
}

(async () => {
    try {
        const client = await MongoClient.connect(DB_URL);
        db = client.db(DB_NAME);
        console.log(`Connected to MongoDB ${DB_NAME}`);
    } catch (error) {
        console.error('Error connecting to MongoDB:', error);
        process.exit(1);
    }
    try {
        const conn = await connect(`amqp://guest:guest@localhost:${PUBLISHER_PORT}`);
        channel = await conn.createChannel();
        await channel.assertQueue(QUEUE_NAME);
        console.log(`Connected to RabbitMQ queue ${QUEUE_NAME}`);
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
        process.exit(1);
    }
    // Start consuming messages in parallel
    await consumeMessages();
    // Start publishing messages in parallel
    await publishMessages();
})();
