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
import java.util.concurrent.TimeUnit;

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
    channel.basicQos(args.getBufferBatchSize());
    logger.info("Bitserv connected to RabbitMQ");
    
    MetricAnalyser.setDebugMode(args.getDebugMode());
    BatchInsertionControl insertionControl = BatchInsertionControl.getInstance(args.getBufferBatchSize(),args.getBufferCapacityFactor());
    ExecutorService executor = Executors.newFixedThreadPool(1);
    eventbus = new AsyncEventBus(executor);
    ExecutorService dispatchExec = Executors.newFixedThreadPool(args.getBufferCapacityFactor());
    eventbus.register(new DispatchBufferListener(dispatchExec,insertionControl));
    ActionHandler.initStaticDependecies(insertionControl,eventbus);
    
    DefaultConsumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
          throws IOException {
        String message = new String(body, "UTF-8");
        logger.info("[X] Message received: " + message);
        new ActionHandler(message).handle();
        
        long deliveryTag = envelope.getDeliveryTag();
        channel.basicAck(deliveryTag, true);
      }
    };
    channel.basicConsume(args.getrmQueue(), false, consumer);
    logger.info("Bitserv connected to BigQuery");
    /*
    * Timer for Dispatch event
    */
    Integer dispatchInterval = args.getBufferDispatchInterval();
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        eventbus.post(new BufferDispatchEvent(true));
      }
    }, dispatchInterval*1000, dispatchInterval*1000);
    /*
    * handle SIGTERM & SIGINT & SIGHUP
    */
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        timer.cancel();
        try{
          String consumerTag = consumer.getConsumerTag();
          channel.basicCancel(consumerTag);
          channel.close();
          connection.close();
        }catch( IOException | TimeoutException e ){
          logger.error("[BITSERVE Shutdown Error] : "+ e.toString());
        }
        dispatchExec.shutdown();
        try {
          dispatchExec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {  
          dispatchExec.shutdownNow();
        }
        System.out.println(insertionControl.toString());
        logger.info("[gracefull shutdown]  Shutdown begin...");
        logger.info("[gracefull shutdown]  Timer shutdown");
        try{
          ActionHandler.dispatchEvent("dispatch.buffer.all","SHUTDOWN-VM");
        }catch(Exception e){
          logger.error("Dispatch error at shutdownhook : " + e);
        }
        System.out.println(insertionControl.toString());
        logger.info("[gracefull shutdown]  Shutdown hook ran!");
        MetricAnalyser.log();
      }
    });
  }
}

class DispatchBufferTask implements Runnable {
    private BatchInsertionControl bufferControl;
    private BufferDispatchEvent event;
    public DispatchBufferTask(BatchInsertionControl bufferControl, BufferDispatchEvent event){
        this.bufferControl = bufferControl;
        this.event = event;
    }
    @Override
    public void run() {
        if(event.dispatchAll) ActionHandler.dispatchEvent("dispatch.buffer.all","DISPATCHER");
        else ActionHandler.dispatchEvent("dispatch.buffer.batch","DISPATCHER");
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
    this.executor.execute(new DispatchBufferTask(this.bufferControl, event));
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

  @Parameter(names = "-BufferBatchSize", description = "Insertion Buffer's Batch Size")
  private Integer bufferBatchSize = 20; // in messages

  @Parameter(names = "-BufferCapacityFactor", description = "Insertion Buffer's capacity as a factor of Batch Size")
  private Integer bufferCapacityFactor = 2; // in seconds

  @Parameter(names = "-BufferDispatchInterval", description = "Dispatch Buffer at regular intervals")
  private Integer bufferDispatchInterval = 10; // in seconds

  @Parameter(names = "-DEBUG", description = "for debugging purpose")
  private Integer debug = 0;

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

  public Integer getBufferBatchSize() {
    return this.bufferBatchSize;
  }

  public Integer getBufferCapacityFactor() {
    return this.bufferCapacityFactor;
  }

  public Integer getBufferDispatchInterval() {
    return this.bufferDispatchInterval;
  }

  public boolean getDebugMode() {
    if(this.debug == 1) return true;
    return false;
  }
}