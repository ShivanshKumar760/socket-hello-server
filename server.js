const express = require('express');
const http = require('http');
const path = require('path');
const os = require('os');
const { Server } = require('socket.io');
const amqp = require('amqplib');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const PORT = process.env.PORT || 3000;
const POD_NAME = process.env.POD_NAME || os.hostname();
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const EXCHANGE = 'hello_broadcast';

app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (req, res) => res.status(200).send('ok'));

let channel;

async function connectRabbit() {
  const conn = await amqp.connect(RABBITMQ_URL);
  channel = await conn.createChannel();

  // fanout = every bound queue gets a copy of every message — this is the
  // "broadcast to all pods" part.
  await channel.assertExchange(EXCHANGE, 'fanout', { durable: false });

  // '' as the name + exclusive:true = RabbitMQ generates a random queue name
  // that only THIS connection can use, and deletes it the moment this pod
  // disconnects. Every pod gets its OWN queue bound to the same exchange —
  // that's what makes it "every pod gets every message" instead of RabbitMQ
  // round-robining one message to whichever pod happens to be free.
  const q = await channel.assertQueue('', { exclusive: true });
  await channel.bindQueue(q.queue, EXCHANGE, '');

  channel.consume(q.queue, (msg) => {
    if (!msg) return;
    const payload = JSON.parse(msg.content.toString());
    // Re-emit to every browser socket THIS pod has open, regardless of
    // which pod originally published it.
    io.emit('hello', payload);
  }, { noAck: true });

  console.log(`[${POD_NAME}] connected to RabbitMQ, bound queue ${q.queue}`);
}

connectRabbit().catch((err) => {
  console.error('RabbitMQ connection failed:', err.message);
  process.exit(1); // crash on purpose — let Kubernetes restart a clean pod
});

io.on('connection', (socket) => {
  console.log(`[${POD_NAME}] client connected: ${socket.id}`);

  // Every 3s, THIS pod publishes a hello naming itself. Every pod (including
  // this one) receives it via the consumer above and re-emits it to its own
  // browsers — so one browser, connected to one pod, ends up seeing hellos
  // from ALL pod names over time.
  const interval = setInterval(() => {
    if (!channel) return;
    channel.publish(EXCHANGE, '', Buffer.from(JSON.stringify({
      message: 'hello',
      pod: POD_NAME,
      time: new Date().toISOString(),
    })));
  }, 3000);

  socket.on('disconnect', () => clearInterval(interval));
});

server.listen(PORT, () => console.log(`[${POD_NAME}] listening on ${PORT} start exploring`));