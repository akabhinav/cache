package com.distributedcache.redis.pubsub;

/**
 * Interface for pub/sub subscribers
 */
public interface Subscriber {
    /**
     * Called when a message is published to a subscribed channel
     */
    void onMessage(String channel, String message);

    /**
     * Called when a message is published to a channel matching a subscribed pattern
     */
    void onPatternMessage(String pattern, String channel, String message);
}
