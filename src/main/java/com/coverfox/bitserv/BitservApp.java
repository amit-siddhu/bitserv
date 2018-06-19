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

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.json.JSONObject;


import com.rabbitmq.client.impl.DefaultExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.Timer;
import java.util.TimerTask;

public class BitservApp {

  private static final Logger logger = LogManager.getLogger(BitservApp.class);
  public static AsyncEventBus eventbus;

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
    
    BatchInsertionControl insertionControl = BatchInsertionControl.getInstance(args.getrmBufferSize());
    ExecutorService executor = Executors.newFixedThreadPool(1);
    eventbus = new AsyncEventBus(executor);
    // ExecutorService bufferExec = Executors.newFixedThreadPool(1);
    // eventbus.register(new AddToBufferListener(bufferExec,insertionControl));
    ExecutorService dispatchExec = Executors.newFixedThreadPool(1);
    eventbus.register(new DispatchBufferListener(dispatchExec,insertionControl));
    ActionHandler.initStaticDependecies(insertionControl,eventbus);
    
    DefaultConsumer consumer = new DefaultConsumer(channel) {
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
    * handle SIGTERM & SIGINT & SIGHUP
    */
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        try{
          String consumerTag = consumer.getConsumerTag();
          System.out.println(consumerTag);
          channel.basicCancel(consumerTag);
          channel.close();
          connection.close();
        }catch( IOException | TimeoutException e ){
          logger.error("[BITSERVE Shutdown Error] : "+ e.toString());
        }

        dispatchExec.shutdown();

        // timer.cancel();
        logger.info("[gracefull shutdown]  Shutdown begin...");
        logger.info("[gracefull shutdown]  Timer shutdown");
        try{
          ActionHandler.dispatchEvent("dispatch.buffer.all","SHUTDOWN-VM");
        }catch(Exception e){
          logger.error("Dispatch error at shutdownhook : " + e);
        }

        //wait for executors to shutodown 

        logger.info("[gracefull shutdown]  Shutdown hook ran!");
      }
    });
  }
}
class AddToBufferTask implements Runnable {
    private JSONObject message;
    private BatchInsertionControl bufferControl;
    public AddToBufferTask(JSONObject msg, BatchInsertionControl bufferControl){
        this.message = msg;
        this.bufferControl = bufferControl;
    }
    @Override
    public void run() {
        // System.out.println("Add to Buffer : " + Thread.currentThread().getName()+" Start." + this.message.toString());
        Integer bufferIndicator = this.bufferControl.buffer(this.message);
        if(bufferIndicator > 10) System.out.println("buffer control check : "+ Integer.toString(bufferIndicator));
        if(this.bufferControl.dispatchReady(bufferIndicator)) {
          System.out.println("pre buffer batch dispatch : "+ Integer.toString(bufferIndicator));
          BitservApp.eventbus.post(new BufferDispatchEvent());
        }
    }
}
class AddToBufferListener {
  private ExecutorService executor;
  private BatchInsertionControl bufferControl;
  public AddToBufferListener(ExecutorService exec, BatchInsertionControl bufferControl){
    this.executor = exec;
    this.bufferControl = bufferControl;
  }
  @Subscribe
  public void task(BSevent event) {
    String eventname = event.getName();
    JSONObject payload = event.getMessage();
    this.executor.execute(new AddToBufferTask(payload,this.bufferControl));
  }
}

class DispatchBufferTask implements Runnable {
    private BatchInsertionControl bufferControl;
    public DispatchBufferTask(BatchInsertionControl bufferControl){
        this.bufferControl = bufferControl;
    }
    @Override
    public void run() {
        System.out.println("[**Dispatch Buffer**] : " + Thread.currentThread().getName());
        ActionHandler.dispatchEvent("dispatch.buffer.batch","THREAD");
    }
}
class DispatchBufferListener {
  private ExecutorService executor;
  private BatchInsertionControl bufferControl;
  public DispatchBufferListener(ExecutorService exec, BatchInsertionControl bufferControl){
    this.executor = exec;
    this.bufferControl = bufferControl;
  }
  @Subscribe
  public void task(BufferDispatchEvent event) {
    this.executor.execute(new DispatchBufferTask(this.bufferControl));
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

  @Parameter(names = "-rmBufferSize", description = "Insertion Buffer Size")
  private Integer rmBufferSize = 20; // in messages

  @Parameter(names = "-rmBufferTime", description = "Insertion Buffer Time")
  private Integer rmBufferTime = 10; // in seconds

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

  public Integer getrmBufferSize() {
    return this.rmBufferSize;
  }

  public Integer getrmBufferTime() {
    return this.rmBufferTime;
  }
}