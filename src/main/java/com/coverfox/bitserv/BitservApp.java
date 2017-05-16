package com.coverfox.bitserv;

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
  private static final String RABBIT_QUEUE_NAME = "hello";

  public static void main(String[] argv) throws Exception {
    logger.info("Booting Bitserv");

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final ExceptionHandler eh = new DefaultExceptionHandler() {
      @Override
      public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag, String methodName) {
        logger.error(exception);
      }
    };
    factory.setExceptionHandler(eh);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare(RABBIT_QUEUE_NAME, false, false, false, null);
    logger.info("Bitserv connected to RabbitMQ");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        String message = new String(body, "UTF-8");
        logger.info("[X] Message received: " + message);
        new ActionHandler(message).handle();

      }
    };
    channel.basicConsume(RABBIT_QUEUE_NAME, true, consumer);
    logger.info("Bitserv connected to BigQuery");
  }
}
