const { Kafka } = require('kafkajs');

// Initialize Kafka client
const kafka = new Kafka({
    clientId: 'my-consumer',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });
const topic = 'test-topic';

const runConsumer = async () => {
    // Connect to the consumer
    await consumer.connect();
    console.log('Consumer connected');

    // Subscribe to the topic
    await consumer.subscribe({ topic, fromBeginning: true });
    console.log(`Subscribed to topic: ${topic}`);

    // Start consuming messages
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const key = message.key ? message.key.toString() : null;
            const value = message.value.toString();

            console.log(`\n*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*`);
            console.log(new Date().toLocaleString());
            console.log({
                topic,
                partition,
                key,
                value,
                timestamp: new Date(parseInt(message.timestamp)).toISOString()
            });
        },
    });
};

// Handle shutdowns gracefully
const shutdown = async () => {
    await consumer.disconnect();
    console.log('Consumer disconnected');
    process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

runConsumer().catch(console.error);