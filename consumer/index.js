import { Kafka, Partitioners } from "kafkajs";
console.log('*** Consumer starts... ***');

const kafka = new Kafka({
  clientId: 'checker-server',
  brokers: ['localhost:9092']
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const consumer = kafka.consumer({ groupId: 'kafka-checker-servers1' });

const ssnTester = (ssn) => {
  // Tarkista hetun muoto
  const regex = /^[0-9]{6}[A+-][0-9]{3}[0-9A-Z]$/;
  if (!regex.test(ssn)) {
    return false;
  }

  // Erottele kaikki merkit
  const day = ssn.slice(0, 2);
  const month = ssn.slice(2, 4);
  const year = ssn.slice(4, 6);
  const separator = ssn.at(6);
  const id = ssn.slice(7, 10);

  // Erottele vuosisata
  let century;
  if (separator === '+') {
    century = 18;
  } else if (separator === '-') {
    century = 19;
  } else if (separator === 'A') {
    century = 20;
  } else return false;

  // Tarkista jos henkilö ei ole vielä syntynyt
  // (Periaatteessa tulevaisuudenkin hetut ovat valideja)
  const fullYear = century + year;
  const birthday = new Date(fullYear, month, day);
  const today = new Date();
  if (birthday > today) return false;

  // Laske jakojäännös ja tarkistusmerkki
  const checksum = day + month + year + id;
  const modulo = parseInt(checksum) % 31;
  const checksumChars = '0123456789ABCDEFHJKLMNPRSTUVWXY';
  const valid = checksumChars.at(modulo);
  return (ssn.at(10) === valid);
}

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true });
  await producer.connect();

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {

      if (ssnTester(message.value.toString())) {
        await producer.send({
          topic: 'checkedresult',
          messages: [
            {
              value: String('SSN is valid'),
            }
          ]
        })
      } else {
        await producer.send({
          topic: 'checkedresult',
          messages: [
            {
              value: String('SSN is not valid'),
            }
          ]
        })
      }

      console.log({
        key: message.key.toString(),
        partition: message.partition,
        offset: message.offset,
        value: message.value.toString(),
        valid: ssnTester(message.value.toString()),
      })
    },
  })
}

run().catch(console.error);
