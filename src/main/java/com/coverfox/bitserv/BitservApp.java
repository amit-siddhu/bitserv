package com.coverfox.bitserv;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.impl.DefaultExceptionHandler;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BitservApp {

  private static final Logger logger = LogManager.getLogger(BitservApp.class);

  public static void main(String[] argv) throws Exception {
    Args args = new Args();
    JCommander.newBuilder()
      .addObject(args)
      .build()
      .parse(argv);

    logger.info("Booting Bitserv");
    ConnectionFactory factory = new ConnectionFactory();

    factory.setHost(args.getrmHost());
    factory.setPort(args.getrmPort());
    factory.setUsername(args.getrmUser());
    factory.setPassword(args.getrmPass());
    factory.setVirtualHost(args.getrmVhost());
    final ExceptionHandler eh = new DefaultExceptionHandler() {
      @Override
      public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
        logger.error(exception);
      }
    };
    factory.setExceptionHandler(eh);

    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(args.getrmQueue(), true, false, false, null);
    logger.info("Bitserv connected to RabbitMQ");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        String message = new String(body, "UTF-8");
        // logger.info("[X] Message received: " + message);
        new ActionHandler(message).handle();

      }
    };
    channel.basicConsume(args.getrmQueue(), true, consumer);
    logger.info("Bitserv connected to BigQuery");
    /*
    * handle SIGTERM & SIGINT
    */
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        System.out.println("[gracefull shutdown]  Shutdown begin...");
        ActionHandler.dispatchEvent("insert.buffer.dispatch");
        System.out.println("[gracefull shutdown]  Shutdown hook ran!");
      }
    });
  }
}

class Args {
  @Parameter(names = "-rmHost", description = "RabbitMQ host address")
  private String rmHost = "localhost";

  @Parameter(names = "-rmPort", description = "RabbitMQ port")
  private Integer rmPort = 5672;

  @Parameter(names = "-rmUser", description = "RabbitMQ username")
  private String rmUser = "guest";

  @Parameter(names = "-rmPass", description = "RabbitMQ password")
  private String rmPass = "guest";

  @Parameter(names = "-rmQueue", description = "RabbitMQ queue name")
  private String rmQueue = "testQueue";

  @Parameter(names = "-rmVhost", description = "RabbitMQ virtual host")
  private String rmVhost = "/";

  public String getrmHost() {
    return this.rmHost;
  }

  public Integer getrmPort() {
    return this.rmPort.intValue();
  }

  public String getrmUser() {
    return this.rmUser;
  }

  public String getrmPass() {
    return this.rmPass;
  }

  public String getrmQueue() {
    return this.rmQueue;
  }

  public String getrmVhost() {
    return this.rmVhost;
  }
}