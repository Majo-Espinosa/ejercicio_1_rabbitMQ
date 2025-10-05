const amqp = require('amqplib');

async function sendMessages() {
  try {
    // Leer usuario y contraseña desde variables de entorno o usar valores por defecto
    const user = process.env.RABBITMQ_DEFAULT_USER || 'guest';
    const pass = process.env.RABBITMQ_DEFAULT_PASS || 'guest';
    const host = process.env.RABBITMQ_HOST || 'rabbitmq'; // nombre del servicio en docker-compose
    const queue = 'tareas_distribuidas';

    // 1. Conexión a RabbitMQ
    const connection = await amqp.connect(`amqp://${user}:${pass}@${host}:5672`);
    const channel = await connection.createChannel();

    // 2. Crear/Asegurar la cola (durable para no perder mensajes)
    await channel.assertQueue(queue, { durable: true });

    console.log('Conectado a RabbitMQ — Enviando tareas...\n');

    // 3. Generar y enviar 10 tareas con distintas complejidades (1–5)
    for (let i = 1; i <= 10; i++) {
      // Nivel de complejidad aleatorio (1 a 5)
      const complejidad = Math.floor(Math.random() * 5) + 1;
      const tarea = {
        id: i,
        complejidad,
        payload: `Tarea #${i}`,
      };

      // Convertir a buffer y enviar mensaje persistente
      channel.sendToQueue(queue, Buffer.from(JSON.stringify(tarea)), {
        persistent: true,
      });

      console.log(`[x] Enviada tarea ${i} (complejidad=${complejidad})`);
    }

    // 4. Esperar un poco antes de cerrar conexión para asegurar envío
    setTimeout(async () => {
      await channel.close();
      await connection.close();
      console.log('\n Productor finalizado, todas las tareas enviadas.');
      process.exit(0);
    }, 500);
  } catch (err) {
    console.error(' Error en Producer:', err);
    process.exit(1);
  }
}

// Ejecutar una vez para enviar todas las tareas
sendMessages();
