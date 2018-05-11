package me.xiaoqian.rabbitmq;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

class RabbitMqUtils {
	
	private final static Logger LOG = LoggerFactory.getLogger(RabbitMqUtils.class);
	
	private static ConnectionFactory connectionFactory;
	
	private static final String RPC_QUEUE_NAME = "rpc_queue";

	
	static {
		connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("localhost");
	}
	
	public  static Connection getConnection() {
		Connection connection = null;
		try {
			connection = connectionFactory.newConnection();
		}catch (Exception e) {
			LOG.error("MQ获取连接错误：",e);
		}
		return connection;
	}
	
	public static Consumer getDefaultConsumer(Channel channel) {
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				Thread currentThread = Thread.currentThread();
				String message = new String(body,"UTF-8");
				LOG.info("【{}】消费了MQ一条消息【{}】",currentThread.getName(),message);
			}
		};
		return consumer;
	}
	public static Consumer getAckConsumer(final Channel channel) {
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
					throws IOException {
				Thread currentThread = Thread.currentThread();
				try {
					currentThread.sleep(500);
					String message = new String(body,"UTF-8");
					LOG.info("【{}】消费了MQ一条消息【{}】",currentThread.getName(),message);
					channel.basicAck(envelope.getDeliveryTag(),false);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		return consumer;
	}
	
	public static boolean one2OneProducer(String queueName,String message){
		if(null != message && !"".equals(message)) {
			try {
				Connection connection = getConnection();
				if(null != connection) {
					Channel channel = connection.createChannel();
					channel.queueDeclare(queueName, false, false, false,null);
					channel.basicPublish("",queueName, null,message.getBytes());
					LOG.info("往MQ【{}】发送了一条消息【{}】",queueName,message);
					channel.close();
					connection.close();
					return true;
				}
			} catch (Exception e) {
				LOG.error("MQ获取渠道错误：",e);
			}
		}
		return false;
	}
	
	public static boolean one2OneConsumer(String queueName) {
		if(!"".equals(queueName)) {
			try {
				Connection connection = getConnection();
				if(null != connection) {
					Channel channel = connection.createChannel();
					channel.queueDeclare(queueName, true, false, false,null);
					Consumer consumer = getDefaultConsumer(channel);
					String str = channel.basicConsume(queueName,true,consumer);
					LOG.info(str);
					LOG.info("等待消息~~~~~~~");
					while(true) {
					}
				}
			} catch (Exception e) {
				LOG.error("MQ获取渠道错误：",e);
			}
		}
		return false;
	}
	
	public static boolean one2MoreProducer(String queueName,String message){
		if(null != message && !"".equals(message)) {
			try {
				Connection connection = getConnection();
				if(null != connection) {
					Channel channel = connection.createChannel();
					//第一个true ：关闭自动确认（开启手工确认） 第二个true:是否是持久化的消息
					channel.queueDeclare(queueName, true, false, false,null);
					channel.basicPublish("",queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());
					LOG.info("往MQ【{}】发送了一条消息【{}】",queueName,message);
					channel.close();
					connection.close();
					return true;
				}
			} catch (Exception e) {
				LOG.error("MQ获取渠道错误：",e);
			}
		}
		return false;
	}
	
	public static boolean one2MoreConsumer(String queueName) {
		if(!"".equals(queueName)) {
			try {
				Connection connection = getConnection();
				if(null != connection) {
					Channel channel = connection.createChannel();
					channel.queueDeclare(queueName, true, false, false,null);
					//每个work分发一个task
					channel.basicQos(1);
					Consumer consumer = getAckConsumer(channel);
					String str = channel.basicConsume(queueName,false,consumer);
					LOG.info(str);
					LOG.info("等待消息~~~~~~~");
					while(true) {
					}
				}
			} catch (Exception e) {
				LOG.error("MQ获取渠道错误：",e);
			}
		}
		return false;
	}
	
	public static int fib(int n) {
		if(n == 0) return 0;
		if(n==1) return 1;
		return fib(n-1)+fib(n-2);
	}
	
	public static void initRpcServer(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = null;
        try {
            connection      = factory.newConnection();
            final Channel channel = connection.createChannel();
            channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
            channel.basicQos(1);
            System.out.println(" [x] Awaiting RPC requests");
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                            .Builder()
                            .correlationId(properties.getCorrelationId())
                            .build();

                    String response = "";

                    try {
                        String message = new String(body,"UTF-8");
                        int n = Integer.parseInt(message);

                        System.out.println(" [.] fib(" + message + ")");
                        response += fib(n);
                    }
                    catch (RuntimeException e){
                        System.out.println(" [.] " + e.toString());
                    }
                    finally {
                        channel.basicPublish( "", properties.getReplyTo(), replyProps, response.getBytes("UTF-8"));

                        channel.basicAck(envelope.getDeliveryTag(), false);

            // RabbitMq consumer worker thread notifies the RPC server owner thread 
                    synchronized(this) {
                        this.notify();
                    }
                    }
                }
            };
            channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
            // Wait and be prepared to consume the message from RPC client.
        while (true) {
            synchronized(consumer) {
        try {
              consumer.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();          
            }
            }
         }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
        try {
                connection.close();
             } catch (IOException _ignore) {}
         }
	}
	
	

}
