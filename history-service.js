import { createServer } from "node:http";
import {Readable} from 'node:stream';
import JSONStream from "JSONStream";
import timestamp from "monotonic-timestamp";
import { Level } from "level";
import amqp from "amqplib";

async function main() {
  const db = new Level("./chat-db");

  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();
  const { exchange } = await channel.assertExchange("chat", "fanout");
  const { queue } = await channel.assertQueue("chat_history");
  await channel.bindQueue(queue, exchange);

  channel.consume(queue, async (msg) => {
    const content = msg.content.toString();
    console.log("Saving content %s", content);
    await db.put(timestamp(), content);
    channel.ack(msg);
  });

  const server = createServer((req, res) => {
    res.statusCode = 200;
    const stream = Readable.from(db.values());
    stream.on('data', (data) => {
        console.log(data);
    })
    stream.pipe(JSONStream.stringify()).pipe(res);
  })
  
  server.listen(process.env.port ?? process.argv[2] ?? 8080, () => {
    const address = server.address()
    console.log('History service running on port %d', address.port);
  });
}


main().catch(err => console.log(err));