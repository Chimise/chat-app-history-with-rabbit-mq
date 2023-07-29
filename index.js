import { createServer } from "node:http";
import ws, { WebSocketServer } from "ws";
import staticHandler from "serve-handler";
import amqp from "amqplib";
import JSONStream from "JSONStream";
import superagent from "superagent";

const port = process.argv[2] || 8090;
const historyPort = process.argv[3] || 8082;

async function main() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();
  const { exchange } = await channel.assertExchange("chat", "fanout");
  const { queue } = await channel.assertQueue(`chat_srv_${port}`, {
    exclusive: true,
  });
  await channel.bindQueue(queue, exchange);

  channel.consume(
    queue,
    (msg) => {
      msg = msg.content.toString();
      console.log(`From queue: ${msg}`);
      broadcast(msg);
    },
    { noAck: true }
  );

  const server = createServer((req, res) => {
    return staticHandler(req, res, { public: "www" });
  });

  const wss = new WebSocketServer({ server });

  wss.on("connection", (client) => {
    console.log("Client connected");
    client.on("message", (msg) => {
      console.log(`Message: ${msg}`);
      channel.publish("chat", "", Buffer.from(msg));
    });

    // Query the history service
    superagent
      .get(`http://localhost:${historyPort}`)
      .on("error", (err) => console.error(err))
      .pipe(JSONStream.parse("*"))
      .on("data", (msg) => client.send(msg));
  });

  function broadcast(message) {
    for (const client of wss.clients) {
      if (client.readyState === ws.OPEN) {
        client.send(message.toString());
      }
    }
  }

  server.listen(port, () => {
    console.log('Chat server running on port %d', port);
  });
}


main().catch(err => console.log(err));