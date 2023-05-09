package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.io.Serializable;

public class Client1 {
    public static void main(String[] args) throws InterruptedException {

        bounded_queue_producer();

    }

    public static void distributed_map() {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        IMap map = hz.getMap("my-distributed-map");

        for (int i = 0; i < 1000; i++) {
            String key = "key - " + i;
            map.put(key, i);
        }

        int size = map.size();
        System.out.println("Map size: " + size);

        hz.shutdown();
    }

    public static void without_locking() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Integer> map = hz.getMap( "my-distributed-map");

        String key = "1";
        map.put( key, 0 );

        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            if ( k % 100 == 0 ) System.out.println( "At: " + k );
            Integer value = map.get( key );
            try {
                Thread.sleep( 10 );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            value++;
            map.put( key, value );
        }

        System.out.println( "Finished! Result = " + map.get(key) );
        hz.shutdown();
    }

    public static void pessimistic_locking() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Integer> map = hz.getMap( "my-distributed-map");

        String key = "1";
        map.put( key, 0 );

        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            if ( k % 100 == 0 ) System.out.println( "At: " + k );
            map.lock(key);
            try {
                Integer value = map.get( key );
                Thread.sleep( 10 );
                value++;
                map.put( key, value );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                map.unlock(key);
            }
        }

        System.out.println( "Finished! Result = " + map.get(key) );
        hz.shutdown();
    }

    public static void optimistic_locking() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        IMap<String, Value> map = hz.getMap( "my-distributed-map");

        String key = "1";
        map.put( key, new Value());

        System.out.println( "Starting" );
        for ( int k = 0; k < 1000; k++ ) {
            if ( k % 100 == 0 ) System.out.println( "At: " + k );
            while (true) {
                try {
                    Value oldValue = map.get(key);
                    Value newValue = new Value(oldValue);
                    Thread.sleep( 10 );
                    newValue.amount++;
                    if (map.replace( key, oldValue, newValue)) {
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        System.out.println( "Finished! Result = " + map.get( key ).amount);
        hz.shutdown();
    }

    public static void destroy_map() {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();
        IMap map = hz.getMap("my-distributed-map");

        map.destroy();
        hz.shutdown();
    }

    static class Value implements Serializable {
        public int amount;

        public Value() {
        }

        public Value( Value that ) {
            this.amount = that.amount;
        }

        public boolean equals( Object o ) {
            if ( o == this ) return true;
            if ( !( o instanceof Value ) ) return false;
            Value that = ( Value ) o;
            return that.amount == this.amount;
        }
    }

    public static void bounded_queue_producer() throws InterruptedException {
        Config config = new Config();
        QueueConfig queueConfig = config.getQueueConfig("bounded-queue");
        queueConfig.setMaxSize(10);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        IQueue<Integer> boundedQueue = hz.getQueue("bounded-queue");

        for ( int k = 1; k < 100; k++ ) {
            boundedQueue.add( k );
            System.out.println( "Producing: " + k );
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        boundedQueue.put( -1 );
        System.out.println( "Producer Finished!" );

        hz.shutdown();
    }
}