package dev.aguxis.conveyor.core;

public record QueueStats(String queue, long pending, long running, long succeeded, long dead) {
}
