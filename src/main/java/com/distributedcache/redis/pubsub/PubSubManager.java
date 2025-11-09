package com.distributedcache.redis.pubsub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Pub/Sub manager for Redis-like publish/subscribe functionality
 */
public class PubSubManager {
    private static final Logger logger = LoggerFactory.getLogger(PubSubManager.class);

    // Channel -> Set of subscribers
    private final Map<String, Set<Subscriber>> channelSubscribers;

    // Pattern -> Set of subscribers
    private final Map<String, Set<Subscriber>> patternSubscribers;

    public PubSubManager() {
        this.channelSubscribers = new ConcurrentHashMap<>();
        this.patternSubscribers = new ConcurrentHashMap<>();
    }

    public void subscribe(Subscriber subscriber, String... channels) {
        for (String channel : channels) {
            channelSubscribers.computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>())
                .add(subscriber);
            logger.debug("Subscriber {} subscribed to channel {}", subscriber, channel);
        }
    }

    public void unsubscribe(Subscriber subscriber, String... channels) {
        if (channels.length == 0) {
            // Unsubscribe from all channels
            channelSubscribers.values().forEach(subs -> subs.remove(subscriber));
        } else {
            for (String channel : channels) {
                Set<Subscriber> subs = channelSubscribers.get(channel);
                if (subs != null) {
                    subs.remove(subscriber);
                    if (subs.isEmpty()) {
                        channelSubscribers.remove(channel);
                    }
                }
            }
        }
    }

    public void psubscribe(Subscriber subscriber, String... patterns) {
        for (String pattern : patterns) {
            patternSubscribers.computeIfAbsent(pattern, k -> new CopyOnWriteArraySet<>())
                .add(subscriber);
            logger.debug("Subscriber {} subscribed to pattern {}", subscriber, pattern);
        }
    }

    public void punsubscribe(Subscriber subscriber, String... patterns) {
        if (patterns.length == 0) {
            // Unsubscribe from all patterns
            patternSubscribers.values().forEach(subs -> subs.remove(subscriber));
        } else {
            for (String pattern : patterns) {
                Set<Subscriber> subs = patternSubscribers.get(pattern);
                if (subs != null) {
                    subs.remove(subscriber);
                    if (subs.isEmpty()) {
                        patternSubscribers.remove(pattern);
                    }
                }
            }
        }
    }

    public int publish(String channel, String message) {
        Set<Subscriber> notified = new HashSet<>();

        // Notify exact channel subscribers
        Set<Subscriber> channelSubs = channelSubscribers.get(channel);
        if (channelSubs != null) {
            for (Subscriber sub : channelSubs) {
                sub.onMessage(channel, message);
                notified.add(sub);
            }
        }

        // Notify pattern subscribers
        for (Map.Entry<String, Set<Subscriber>> entry : patternSubscribers.entrySet()) {
            String pattern = entry.getKey();
            if (matchPattern(pattern, channel)) {
                for (Subscriber sub : entry.getValue()) {
                    sub.onPatternMessage(pattern, channel, message);
                    notified.add(sub);
                }
            }
        }

        int count = notified.size();
        logger.debug("Published message to channel {}: {} subscribers notified", channel, count);
        return count;
    }

    private boolean matchPattern(String pattern, String channel) {
        // Simple pattern matching (supports * and ? wildcards)
        String regex = pattern
            .replace("*", ".*")
            .replace("?", ".");
        return channel.matches(regex);
    }

    public int getChannelSubscriberCount(String channel) {
        Set<Subscriber> subs = channelSubscribers.get(channel);
        return subs != null ? subs.size() : 0;
    }

    public List<String> getActiveChannels(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return new ArrayList<>(channelSubscribers.keySet());
        }

        return channelSubscribers.keySet().stream()
            .filter(channel -> matchPattern(pattern, channel))
            .toList();
    }
}
