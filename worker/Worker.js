import amqp from "amqplib";

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || "rabbitmq";
const WORKER_NAME = process.env.WORKER_NAME || "Worker";
const CLAVES = (process.env.CLAVES || "").split(",");

(async () => {
  try {
    const conn = await amqp.connect(`amqp://${RABBITMQ_HOST}`);
    const ch = await conn.createChannel();
    const exchange = "direct_logs";

    await ch.assertExchange(exchange, "direct", { durable: false });
    const q = await ch.assertQueue("", { exclusive: true });

    for (const clave of CLAVES) {
      await ch.bindQueue(q.queue, exchange, clave);
      console.log(`[${WORKER_NAME}] Suscrito a tareas de tipo '${clave}'`);
    }

    console.log(`[${WORKER_NAME}] Iniciado y listo para procesar tareas suscrito al exchange '${exchange}'\n`);

    ch.consume(q.queue, async (msg) => {
      if (msg.content) {
        const tarea = JSON.parse(msg.content.toString());
        console.log(`[${WORKER_NAME}] Recibió tarea ${tarea.id} (complejidad: ${tarea.complejidad}, tipo: ${tarea.tipo})`);

        await new Promise((resolve) => setTimeout(resolve, tarea.complejidad * 1000));

        console.log(`[${WORKER_NAME}] Tarea completada: ${tarea.id}\n`);
      }
    }, { noAck: true });

  } catch (err) {
    console.error(`Error en ${WORKER_NAME}:`, err);
  }
})();
