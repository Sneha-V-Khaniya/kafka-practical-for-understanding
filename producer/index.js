const { Kafka } = require('kafkajs');

// Initialize Kafka client
const kafka = new Kafka({
    clientId: 'my-producer',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const topic = 'test-topic';

const runProducer = async () => {
    // Connect to the producer
    await producer.connect();
    console.log('Producer connected');

    let counter = 0;

    // Send a message every second
    setInterval(async () => {
        try {
            const message = {
                key: `key-${counter}`,
                value: `Message ${counter} at ${new Date().toISOString()}`
            };

            await producer.send({
                topic,
                messages: [message],
            });
            console.log(`\n*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*`);
            console.log(new Date().toLocaleString());
            console.log(`Sent message: ${message.value}`);
            counter++;
        } catch (error) {
            console.error('Error producing message:', error);
        }
    }, 15000);
};

// Handle shutdowns gracefully
const shutdown = async () => {
    await producer.disconnect();
    console.log('Producer disconnected');
    process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

runProducer().catch(console.error);