const amqp = require('amqplib');

async function sendMessage() {
  try {
    // 1. Conexión a RabbitMQ
    const connection = await amqp.connect('amqp://rabbitmq');
    const channel = await connection.createChannel();

    // 2. Crear/Asegurar la cola
    const queue = 'tareas_distribuidas';
    await channel.assertQueue(queue, { durable: true });

    // 3. Enviar un mensaje
    const msg = "Hola, soy un mensaje desde Producer!";
    channel.sendToQueue(queue, Buffer.from(msg), { persistent: true });

    console.log(" [x] Enviado:", msg);

    // 4. Cerrar conexión
    setTimeout(() => {
      connection.close();
      process.exit(0);
    }, 500);
  } catch (err) {
    console.error("Error en Producer:", err);
  }
}

sendMessage();
