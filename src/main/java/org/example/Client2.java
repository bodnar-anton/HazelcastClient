package org.example;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Client2 {

    public static void main(String[] args) throws InterruptedException {

        bounded_queue_consumer();

    }

    public static void bounded_queue_consumer() throws InterruptedException {
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig("bounded-queue");
        queueConfig.setMaxSize(10);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        IQueue<Integer> boundedQueue = hz.getQueue("bounded-queue");

        while ( true ) {
            int item = boundedQueue.take();
            System.out.println( "Consumed: " + item );
            Thread.sleep( 1000 );

            if ( item == -1 ) {
                boundedQueue.put( -1 );
                break;
            }
            Thread.sleep( 5000 );
        }
    }

}
