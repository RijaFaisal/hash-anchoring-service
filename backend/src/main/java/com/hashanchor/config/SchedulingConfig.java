package com.hashanchor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} turns on Spring's background task scheduler,
 * which is what makes {@code @Scheduled} methods elsewhere in the app
 * (currently just {@link com.hashanchor.messaging.OutboxRelay}) actually
 * run. It's off by default — Spring doesn't assume every app wants a
 * scheduler thread pool running.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {}
