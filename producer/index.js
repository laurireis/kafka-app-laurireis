import { Kafka, Partitioners } from "kafkajs";
import { v4 as UUID } from "uuid";
console.log('*** Producer starts... ***');

const kafka = new Kafka({
  clientId: 'my-checking-client',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const run = async () => {
  await producer.connect();

  setInterval(() => {
    queueMessage();
  }, 2500)
}
run().catch(console.error);

const idNumbers = [
  // Väärät
  'NNN588+9999',
  '112233-9999',
  '300233-9999',
  '30233-9999',
  '171232B9330',
  // Oikeat
  '010105A983E',
  '171232A9330',
  '180408A920K',
  '190301A990V',
  '050262+9449',
];

function randomizeIntegerBetween(from, to) {
  return (Math.floor(Math.random() * (to - from + 1))) + from;
}

async function queueMessage() {
  const uuidFraction = UUID().substring(0, 4);

  const success = await producer.send({
    topic: 'tobechecked',
    messages: [
      {
        key: uuidFraction,
        value: Buffer.from(idNumbers[randomizeIntegerBetween(0, idNumbers.length - 1)]),
        valid: Boolean,
      },
    ],
  });

  if (success) {
    console.log(`Message ${uuidFraction} successfully to the stream`);
  } else {
    console.log('Problem writing to stream');
  }

};
