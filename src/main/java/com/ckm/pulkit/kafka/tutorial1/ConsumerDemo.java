package com.ckm.pulkit.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {

    public static void main(String[] args) {

        new ConsumerDemo().run();
    }

    private ConsumerDemo(){

    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-app";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);


        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);

        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();


        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook ");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try{
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

        public class ConsumerRunnable implements Runnable {

            private CountDownLatch latch;
            private KafkaConsumer<String, String> consumer;
            private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());


            public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic) {
                this.latch = latch;

                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


                consumer = new KafkaConsumer<String, String>(properties);
                consumer.subscribe(Arrays.asList(topic));
            }

            @Override
            public void run() {

                try {

                    while(true){
                        ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                        for (ConsumerRecord<String, String> record : records ){

                            logger.info("Key:" + record.key() + ", Value:" + record.value());
                            logger.info("Partition:" + record.partition() + ", Offset: " + record.offset());

                        }
                    }
                } catch (WakeupException e) {
                    logger.info("Received shutdown signal!");
                } finally {
                    consumer.close();
                    latch.countDown();
                }



            }

            public void shutdown(){
                consumer.wakeup();
            }
        }
}
