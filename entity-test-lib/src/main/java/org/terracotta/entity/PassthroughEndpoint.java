package org.terracotta.entity;

import com.google.common.util.concurrent.Futures;
import com.tc.entity.Request;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * @author twu
 */
public class PassthroughEndpoint implements EntityClientEndpoint {
  private final ServerEntity entity;

  public PassthroughEndpoint(ServerEntity entity) {
    this.entity = entity;
  }

  @Override
  public byte[] getEntityConfiguration() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void registerListener(EndpointListener listener) {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public InvocationBuilder beginInvoke() {
    return new InvocationBuilderImpl();
  }

  private class FakeSourceID implements SourceID {
  }

  private class InvocationBuilderImpl implements InvocationBuilder {
    private byte[] payload = null;
    private final Set<Request.Acks> acks = EnumSet.noneOf(Request.Acks.class);

    @Override
    public InvocationBuilder ackReceipt() {
      acks.add(Request.Acks.RECEIPT);
      return this;
    }

    @Override
    public InvocationBuilder ackReplicated() {
      acks.add(Request.Acks.REPLICATED);
      return this;
    }

    @Override
    public InvocationBuilder ackLogged() {
      acks.add(Request.Acks.PERSIST_IN_SEQUENCER);
      return this;
    }

    @Override
    public InvocationBuilder ackCompleted() {
      acks.add(Request.Acks.APPLIED);
      return this;
    }

    @Override
    public InvocationBuilder payload(byte[] payload) {
      this.payload = payload;
      return this;
    }

    @Override
    public Future<byte[]> invoke() {
      try {
        return Futures.immediateFuture(entity.invoke(new FakeSourceID(), payload));
      } catch (Exception e) {
        return Futures.immediateFailedCheckedFuture(e);
      }
    }
  }
}
