const amqp = require('amqplib');

async function receiveMessages() {
  try {
    // Configuración desde variables de entorno
    const user = process.env.RABBITMQ_DEFAULT_USER || 'guest';
    const pass = process.env.RABBITMQ_DEFAULT_PASS || 'guest';
    const host = process.env.RABBITMQ_HOST || 'rabbitmq';
    const exchange = 'logs_complejidad';
    const workerName = process.env.WORKER_NAME || 'worker';

    // 1️⃣ Conexión a RabbitMQ
    const connection = await amqp.connect(`amqp://${user}:${pass}@${host}:5672`);
    const channel = await connection.createChannel();

    // 2️⃣ Declarar exchange tipo fanout
    await channel.assertExchange(exchange, 'fanout', { durable: false });

    // 3️⃣ Crear cola temporal exclusiva para este worker
    const q = await channel.assertQueue('', { exclusive: true });

    // 4️⃣ Enlazar la cola al exchange
    await channel.bindQueue(q.queue, exchange, '');

    console.log(`🐇 [${workerName}] Esperando tareas en exchange "${exchange}"...\n`);

    // 5️⃣ Consumir mensajes (todos los workers recibirán todas las tareas)
    channel.consume(
      q.queue,
      async (msg) => {
        if (msg) {
          const data = JSON.parse(msg.content.toString());
          const { id, complejidad, payload } = data;

          console.log(
            `[${workerName}] Recibió tarea #${id} (${payload}) — complejidad=${complejidad}`
          );

          // Simular procesamiento proporcional a la complejidad
          await new Promise((resolve) => setTimeout(resolve, complejidad * 1000));

          console.log(`[${workerName}] ✅ Completó tarea #${id} (complejidad=${complejidad})\n`);
        }
      },
      { noAck: true } // fanout: sin ACK ni persistencia
    );

    // Cierre ordenado
    process.on('SIGINT', async () => {
      console.log(`[${workerName}] Cerrando conexión...`);
      await channel.close();
      await connection.close();
      process.exit(0);
    });
  } catch (err) {
    console.error('❌ Error en Worker:', err);
    process.exit(1);
  }
}

// Ejecutar el consumidor
receiveMessages();
