import amqp from "amqplib";

async function sendMessages() {
  const user = process.env.RABBITMQ_DEFAULT_USER || "guest";
  const pass = process.env.RABBITMQ_DEFAULT_PASS || "guest";
  const host = process.env.RABBITMQ_HOST || "rabbitmq";
  const exchange = "direct_logs"; 

  try {
    const connection = await amqp.connect(`amqp://${user}:${pass}@${host}:5672`);
    const channel = await connection.createChannel();

    // Exchange tipo direct
    await channel.assertExchange(exchange, "direct", { durable: false });

    console.log("[PRODUCER] Generando 10 tareas...\n");

    // Tipos (claves) de mensajes
    const tipos = ["tarea_aburrida_distribuidos:(", "tarea_horrible"];

    for (let i = 1; i <= 10; i++) {
      const tipo = tipos[Math.floor(Math.random() * tipos.length)]; 
      const complejidad = Math.floor(Math.random() * 5) + 1;
      const id = Math.floor(Math.random() * 10000);

      const mensaje = {
        id,
        complejidad,
        tipo,
      };

      //  Enviamos al exchange con la clave (routing key)
      channel.publish(exchange, tipo, Buffer.from(JSON.stringify(mensaje)));

      console.log(
        `[PRODUCER] Tarea enviada: ID ${id}, Complejidad: ${complejidad}, Tipo: ${tipo}`
      );
    }

    console.log("\n[PRODUCER] Todas las tareas han sido enviadas.\n");

    setTimeout(async () => {
      await channel.close();
      await connection.close();
      process.exit(0);
    }, 500);
  } catch (err) {
    console.error(" Error en Producer:", err);
    process.exit(1);
  }
}

sendMessages();
