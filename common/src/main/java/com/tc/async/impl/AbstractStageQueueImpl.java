package com.tc.async.impl;

import org.slf4j.Logger;

import com.tc.async.api.EventHandlerException;
import com.tc.async.api.Sink;
import com.tc.async.api.Source;
import com.tc.exception.TCRuntimeException;
import com.tc.logging.TCLoggerProvider;
import com.tc.util.Assert;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author cschanck
 **/
public abstract class AbstractStageQueueImpl<EC> implements StageQueue<EC> {

  private volatile boolean closed = false;  // open at create
  private volatile boolean extraStats = true;  
  private final MonitoringEventCreator<EC> monitoring;
  private final EventCreator<EC> creator;
  final Logger logger;
  final String stageName;
  
  public AbstractStageQueueImpl(TCLoggerProvider loggerProvider, String stageName, EventCreator<EC> creator) {
    this.logger = loggerProvider.getLogger(Sink.class.getName() + ": " + stageName);
    this.stageName = stageName;
    this.creator = creator;
    this.monitoring = new MonitoringEventCreator<>(stageName, creator);
  }
  
  abstract SourceQueue[] getSources();

  @Override
  public void enableAdditionalStatistics(boolean track) {
    extraStats = track;
  }
  
  final Event createEvent(EC context) {
    return (extraStats) ? this.monitoring.createEvent(context) : creator.createEvent(context);
  }
    
  Logger getLogger() {
    return logger;
  }
  
  boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    Assert.assertFalse(this.closed);
    this.closed = true;
    for (SourceQueue q : this.getSources()) {
      try {
        q.put(new CloseEvent());
      } catch (InterruptedException ie) {
        logger.debug("closing stage", ie);
        Thread.currentThread().interrupt();
      }
    }
  }
  
  public Map<String, ?> getState() {
    Map<String, Object> queueState = new LinkedHashMap<>();
    if (extraStats) {
      Map<String, ?> stats = this.monitoring.getState();
      if (!stats.isEmpty()) {
        queueState.put("stats", stats);
      }
    }
    return queueState;
  }
  
  public static abstract class SourceQueue implements Source {
    private final BlockingQueue<Event> queue;
    private volatile boolean spinNext = true;
    
    public SourceQueue(BlockingQueue<Event> queue) {
      this.queue = queue;
    }
    
    // XXX: poor man's clear.
    public int clear() {
      int cleared = 0;
      try {
        while (poll(0) != null) {
          cleared++;
        }
        return cleared;
      } catch (InterruptedException e) {
        throw new TCRuntimeException(e);
      }
    }

    @Override
    public boolean isEmpty() {
      return this.queue.isEmpty();
    }

    public void put(Event context) throws InterruptedException {
      this.queue.put(context);
    }

    @Override
    public int size() {
      return this.queue.size();
    }
    
    @Override
    public Event poll(long timeout) throws InterruptedException {
      if (timeout == 0) {
        return this.queue.poll();
      }
      Event e = null;
      long spin = timeout / 10;
      if (spinNext && spin > 20) {
        e = spinPoll(TimeUnit.MILLISECONDS.toNanos(spin));
      }
      if (e == null) {
        e = this.queue.poll(timeout - spin, TimeUnit.MILLISECONDS);
      }
      spinNext = (e != null);
      return e;
    }

    private Event spinPoll(long timeInNanos) {
      long nano = System.nanoTime();
      long sleep = 1L;
      long left = timeInNanos;
      boolean interrupted = false;
      int spinCount = 0;
      try {
        while (left > 0) {
          Event e = queue.poll();
          if (e != null) {
            return e;
          } else if (spinCount < 100) {
            spinCount += 1;
          } else {
            try {
              left = timeInNanos - (System.nanoTime() - nano);
              if (left > 0) {
                long rest = Math.min(left, sleep);
                Thread.sleep(rest / TimeUnit.MILLISECONDS.toNanos(1), (int)(rest % TimeUnit.MILLISECONDS.toNanos(1)));
                sleep = sleep << 1;
              }
            } catch (InterruptedException i) {
              interrupted = true;
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      return null;
    }
  }

  class HandledEvent<C> implements Event {
    private final Event event;

    public HandledEvent(Event event) {
      this.event = event;
    }

    @Override
    public void call() throws EventHandlerException {
      event.call();
    }
  }
  
  
  static class CloseEvent<C> implements Event {

    public CloseEvent() {
    }

    @Override
    public void call() throws EventHandlerException {

    }

  }
}
