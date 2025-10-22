const amqp = require('amqplib');

async function sendMessages() {
  try {
    // Leer usuario y contraseña desde variables de entorno o usar valores por defecto
    const user = process.env.RABBITMQ_DEFAULT_USER || 'guest';
    const pass = process.env.RABBITMQ_DEFAULT_PASS || 'guest';
    const host = process.env.RABBITMQ_HOST || 'rabbitmq'; // nombre del servicio en docker-compose
    const exchange = 'logs_complejidad'; // exchange tipo fanout

    // 1️⃣ Conexión a RabbitMQ
    const connection = await amqp.connect(`amqp://${user}:${pass}@${host}:5672`);
    const channel = await connection.createChannel();

    // 2️⃣ Crear/Asegurar el exchange tipo fanout
    await channel.assertExchange(exchange, 'fanout', { durable: false });

    console.log('🚀 Conectado a RabbitMQ — Enviando tareas a todos los workers...\n');

    // 3️⃣ Generar y enviar 10 tareas con distintas complejidades
    for (let i = 1; i <= 10; i++) {
      const complejidad = Math.floor(Math.random() * 5) + 1;
      const tarea = {
        id: i,
        complejidad,
        payload: `Tarea #${i}`,
      };

      // Publicar mensaje al exchange
      channel.publish(exchange, '', Buffer.from(JSON.stringify(tarea)));

      console.log(`[x] Enviada tarea ${i} (complejidad=${complejidad})`);
    }

    // 4️⃣ Esperar un poco antes de cerrar conexión
    setTimeout(async () => {
      await channel.close();
      await connection.close();
      console.log('\n✅ Productor finalizado, todas las tareas enviadas.');
      process.exit(0);
    }, 500);
  } catch (err) {
    console.error('❌ Error en Producer:', err);
    process.exit(1);
  }
}

// Ejecutar el productor
sendMessages();
