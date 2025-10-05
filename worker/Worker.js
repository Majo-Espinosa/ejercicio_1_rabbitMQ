const amqp = require('amqplib');

async function receiveMessage() {
  try {
    // 1. Conexión a RabbitMQ
    const connection = await amqp.connect('amqp://rabbitmq');
    const channel = await connection.createChannel();

    // 2. Asegurar que la cola existe
    const queue = 'tareas_distribuidas';
    await channel.assertQueue(queue, { durable: true });

    console.log(" [*] Esperando mensajes en %s. Para salir presiona CTRL+C", queue);

    // 3. Recibir mensajes
    channel.consume(queue, (msg) => {
      if (msg !== null) {
        console.log(" [x] Recibido:", msg.content.toString());

        // Simulación de procesamiento
        setTimeout(() => {
          console.log(" [✓] Procesado:", msg.content.toString());
          channel.ack(msg); // confirmar mensaje
        }, 2000);
      }
    }, { noAck: false });
  } catch (err) {
    console.error("Error en Worker:", err);
  }
}

receiveMessage();
