const amqp = require('amqplib');

async function receiveMessages() {
  try {
    // Configuración desde variables de entorno
    const user = process.env.RABBITMQ_DEFAULT_USER || 'guest';
    const pass = process.env.RABBITMQ_DEFAULT_PASS || 'guest';
    const host = process.env.RABBITMQ_HOST || 'rabbitmq';
    const queue = 'tareas_distribuidas';
    const workerName = process.env.WORKER_NAME || 'worker';
    const crashOn = process.env.CRASH_ON ? Number(process.env.CRASH_ON) : null;

    // 1. Conexión a RabbitMQ
    const connection = await amqp.connect(`amqp://${user}:${pass}@${host}:5672`);
    const channel = await connection.createChannel();

    // 2. Asegurar la cola durable
    await channel.assertQueue(queue, { durable: true });

    // 3. Aplicar prefetch = 1 (distribución equitativa entre workers)
    await channel.prefetch(1);

    console.log(`🐇 [${workerName}] Esperando tareas en "${queue}" (prefetch=1)...`);

    // 4. Consumir mensajes
    channel.consume(
      queue,
      async (msg) => {
        if (msg !== null) {
          const data = JSON.parse(msg.content.toString());
          const { id, complejidad, payload } = data;

          console.log(
            `[${workerName}] Recibió tarea #${id} (${payload}) — complejidad=${complejidad}`
          );
          const randomFailure = Math.floor(Math.random() * 5) + 1;
          // Simulación de fallo antes del ack
          if (randomFailure === complejidad) {
            console.log(`[${workerName}] Simulando fallo en tarea ${id} (no se enviará ack)`);
            process.exit(1); // el mensaje se reentregará a otro worker
          }

          // Simulación de procesamiento proporcional a la complejidad
          await new Promise((resolve) => setTimeout(resolve, complejidad * 1000));

          console.log(`[${workerName}] Completó tarea #${id} (complejidad=${complejidad})`);
          channel.ack(msg);
        }
      },
      { noAck: false }
    );

    // Cierre ordenado
    process.on('SIGINT', async () => {
      console.log(`[${workerName}] Cerrando conexión...`);
      await channel.close();
      await connection.close();
      process.exit(0);
    });
  } catch (err) {
    console.error('Error en Worker:', err);
    process.exit(1);
  }
}

receiveMessages();
