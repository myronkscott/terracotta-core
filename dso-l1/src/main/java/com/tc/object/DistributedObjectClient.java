/*
 *
 *  The contents of this file are subject to the Terracotta Public License Version
 *  2.0 (the "License"); You may not use this file except in compliance with the
 *  License. You may obtain a copy of the License at
 *
 *  http://terracotta.org/legal/terracotta-public-license.
 *
 *  Software distributed under the License is distributed on an "AS IS" basis,
 *  WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 *  the specific language governing rights and limitations under the License.
 *
 *  The Covered Software is Terracotta Core.
 *
 *  The Initial Developer of the Covered Software is
 *  Terracotta, Inc., a Software AG company
 *
 */
package com.tc.object;

import com.tc.async.api.EventHandler;
import com.tc.async.api.EventHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tc.async.api.PostInit;
import com.tc.async.api.SEDA;
import com.tc.async.api.Sink;
import com.tc.async.api.Stage;
import com.tc.async.api.StageManager;
import com.tc.entity.DiagnosticMessageImpl;
import com.tc.entity.DiagnosticResponseImpl;
import com.tc.entity.NetworkVoltronEntityMessageImpl;
import com.tc.entity.VoltronEntityAppliedResponseImpl;
import com.tc.entity.VoltronEntityMultiResponse;
import com.tc.entity.VoltronEntityReceivedResponseImpl;
import com.tc.entity.VoltronEntityResponse;
import com.tc.entity.VoltronEntityRetiredResponseImpl;
import com.tc.lang.TCThreadGroup;
import com.tc.logging.ClientIDLogger;
import com.tc.logging.ClientIDLoggerProvider;
import com.tc.net.CommStackMismatchException;
import com.tc.net.MaxConnectionsExceededException;
import com.tc.net.TCSocketAddress;
import com.tc.net.protocol.NetworkStackHarnessFactory;
import com.tc.net.protocol.PlainNetworkStackHarnessFactory;
import com.tc.net.protocol.tcm.ChannelEvent;
import com.tc.net.protocol.tcm.TCMessageHydrateSink;
import com.tc.net.protocol.tcm.ChannelEventListener;
import com.tc.net.protocol.tcm.ClientMessageChannel;
import com.tc.net.protocol.tcm.CommunicationsManager;
import com.tc.net.protocol.tcm.MessageMonitor;
import com.tc.net.protocol.tcm.MessageMonitorImpl;
import com.tc.net.protocol.tcm.TCMessage;
import com.tc.net.protocol.tcm.TCMessageRouter;
import com.tc.net.protocol.tcm.TCMessageRouterImpl;
import com.tc.net.protocol.tcm.TCMessageType;
import com.tc.net.protocol.transport.HealthCheckerConfig;
import com.tc.net.protocol.transport.HealthCheckerConfigClientImpl;
import com.tc.net.protocol.transport.NullConnectionPolicy;
import com.tc.net.protocol.transport.ReconnectionRejectedHandlerL1;
import com.tc.net.protocol.transport.TransportHandshakeException;
import com.tc.object.handler.ClientCoordinationHandler;
import com.tc.object.handshakemanager.ClientHandshakeManager;
import com.tc.object.handshakemanager.ClientHandshakeManagerImpl;
import com.tc.object.msg.ClientHandshakeAckMessageImpl;
import com.tc.object.msg.ClientHandshakeMessageImpl;
import com.tc.object.msg.ClientHandshakeRefusedMessageImpl;
import com.tc.object.msg.ClientHandshakeResponse;
import com.tc.object.msg.ClusterMembershipMessage;
import com.tc.object.request.MultiRequestReceiveHandler;
import com.tc.object.request.RequestReceiveHandler;
import com.tc.object.session.SessionManager;
import com.tc.object.session.SessionManagerImpl;
import com.tc.cluster.ClientChannelEventController;
import com.tc.properties.TCProperties;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import com.tc.stats.counter.CounterManager;
import com.tc.stats.counter.CounterManagerImpl;
import com.tc.text.MapListPrettyPrint;
import com.tc.util.Assert;
import com.tc.util.CommonShutDownHook;
import com.tc.util.TCTimeoutException;
import com.tc.util.UUID;
import com.tc.util.concurrent.SetOnceFlag;
import com.tc.util.concurrent.SetOnceRef;
import com.tc.util.sequence.Sequence;
import com.tc.util.sequence.SimpleSequence;
import com.tc.entity.DiagnosticResponse;
import com.tc.entity.LinearVoltronEntityMultiResponse;
import com.tc.entity.ReplayVoltronEntityMultiResponse;
import com.tc.logging.CallbackOnExitState;
import com.tc.net.basic.BasicConnectionManager;
import com.tc.net.core.ProductID;
import com.tc.util.ProductInfo;
import com.tc.net.core.TCConnectionManager;
import com.tc.net.core.TCConnectionManagerImpl;
import com.tc.net.protocol.tcm.TCMessageHydrateAndConvertSink;
import com.tc.object.msg.ClientHandshakeMessage;
import com.tc.object.msg.ClientHandshakeMessageFactory;
import com.tc.text.PrettyPrintable;
import com.tc.text.PrettyPrinter;
import com.tc.util.concurrent.ThreadUtil;
import com.tc.util.runtime.ThreadDumpUtil;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * This is the main point of entry into the DSO client.
 */
public class DistributedObjectClient {

  protected static final Logger DSO_LOGGER = LoggerFactory.getLogger(DistributedObjectClient.class);
  
  private final ClientBuilder                        clientBuilder;
  private final Iterable<InetSocketAddress> serverAddresses;
  private final TCThreadGroup                        threadGroup;

  private ClientMessageChannel                       channel;
  private TCConnectionManager                        connectionManager;
  private CommunicationsManager                      communicationsManager;
  private ClientHandshakeManager                     clientHandshakeManager;

  private CounterManager                             counterManager;

  private final String                                 uuid;
  private final String                               name;

  private final ClientShutdownManager                shutdownManager = new ClientShutdownManager(this);

  private final SetOnceFlag                          clientStopped                       = new SetOnceFlag();
  private final SetOnceFlag                          connectionMade                      = new SetOnceFlag();
  private final SetOnceRef<Thread>                   connectionThread                    = new SetOnceRef<>();
  private final SetOnceRef<Exception>                exceptionMade                       = new SetOnceRef<>();
 
  private ClientEntityManager clientEntityManager;
  private final StageManager communicationStageManager;
  
  private final boolean isAsync;

  
  public DistributedObjectClient(Iterable<InetSocketAddress> serverAddresses, TCThreadGroup threadGroup,
                                 Properties properties) {
    this(serverAddresses, new StandardClientBuilderFactory("terracotta").create(properties), threadGroup,
         UUID.NULL_ID.toString(), "", false);
  }

  public DistributedObjectClient(Iterable<InetSocketAddress> serverAddresses, ClientBuilder builder, TCThreadGroup threadGroup,
                                 String uuid, String name, boolean asyncDrive) {
    Assert.assertNotNull(serverAddresses);
    this.serverAddresses = serverAddresses;
    this.threadGroup = threadGroup;
    this.clientBuilder = builder;
    this.uuid = uuid;
    this.name = name;
    this.isAsync = asyncDrive;
    
    // We need a StageManager to create the SEDA stages used for handling the messages.
    final SEDA seda = new SEDA(threadGroup);
    communicationStageManager = seda.getStageManager();
    Reference<DistributedObjectClient> ref = new WeakReference<>(this);
    threadGroup.addCallbackOnExitDefaultHandler((state)->{
      DistributedObjectClient ce = ref.get();
      if (ce != null) {
        ce.dump();
        ce.shutdown();
      }
    });
  }

  public boolean isShutdown() {
    return this.clientStopped.isSet();
  }

  public boolean connectFor(long timeout, TimeUnit units) throws InterruptedException {
    final TCProperties tcProperties = TCPropertiesImpl.getProperties();
    final int socketConnectTimeout = tcProperties.getInt(TCPropertiesConsts.L1_SOCKET_CONNECT_TIMEOUT);

    if (socketConnectTimeout < 0) { throw new IllegalArgumentException("invalid socket time value: "
                                                                       + socketConnectTimeout); }

    ClientMessageChannel client = internalStart(socketConnectTimeout);
    connectionThread.set(new Thread(threadGroup, ()->{
          while (!connectionMade.isSet() && !clientStopped.isSet() && !exceptionMade.isSet()) {
            connectionSequence(client);
          }
          //  don't reset interrupted, thread is done
        }, "Connection Maker - " + uuid));
      connectionThread.get().start();

    try {
      return waitForConnection(timeout, units);
    } catch (InterruptedException | RuntimeException | Error e) {
      shutdown();
      throw e;
    }
  }

  public boolean connectOnce(int socketTimeout) {
    try {
      if (!directConnect(internalStart(socketTimeout))) {
        shutdown();
        return false;
      } else {
        return true;
      }
    } catch (RuntimeException | Error t) {
      shutdown();
      throw t;
    }
  }

  private synchronized ClientMessageChannel internalStart(int socketTimeout) {
    final TCProperties tcProperties = TCPropertiesImpl.getProperties();
    final int maxSize = tcProperties.getInt(TCPropertiesConsts.L1_SEDA_STAGE_SINK_CAPACITY);

    final SessionManager sessionManager = new SessionManagerImpl(new SessionManagerImpl.SequenceFactory() {
      @Override
      public Sequence newSequence() {
        return new SimpleSequence();
      }
    });
//  weak reference to allow garbage collection if ref is dropped    
    Reference<DistributedObjectClient> ref = new WeakReference<>(this);
    this.threadGroup.addCallbackOnExitDefaultHandler((CallbackOnExitState state) -> {
      DistributedObjectClient client = ref.get();
      if (client != null) {
        DSO_LOGGER.info(client.getClientState());
      }
      Thread.dumpStack();
    });

    final NetworkStackHarnessFactory networkStackHarnessFactory = new PlainNetworkStackHarnessFactory();

    this.counterManager = new CounterManagerImpl();
    final MessageMonitor mm = MessageMonitorImpl.createMonitor(tcProperties, DSO_LOGGER);
    final TCMessageRouter messageRouter = new TCMessageRouterImpl();
    final HealthCheckerConfig hc = new HealthCheckerConfigClientImpl(tcProperties
                                         .getPropertiesFor(TCPropertiesConsts.L1_L2_HEALTH_CHECK_CATEGORY), "TC Client");

    this.connectionManager = (isAsync) ?
            new TCConnectionManagerImpl(CommunicationsManager.COMMSMGR_CLIENT, 0, this.clientBuilder.createBufferManagerFactory())
            :
            new BasicConnectionManager(name + "/" + uuid, this.clientBuilder.createBufferManagerFactory());
    this.communicationsManager = this.clientBuilder
        .createCommunicationsManager(mm,
                                     messageRouter,
                                     networkStackHarnessFactory,
                                     new NullConnectionPolicy(),
                                     connectionManager,
                                     hc,
                                     getMessageTypeClassMapping(),
                                     ReconnectionRejectedHandlerL1.SINGLETON);

    DSO_LOGGER.debug("Created CommunicationsManager.");

    ClientMessageChannel clientChannel = this.clientBuilder.createClientMessageChannel(this.communicationsManager,
                                                                 sessionManager, socketTimeout);
    this.channel = clientChannel;
    // add this listener so that the whole system is shutdown
    // if the transport is closed from underneath.
    //  this typically happens when the transport is disconnected and 
    // reconnect is disabled
    clientChannel.addListener(new ChannelEventListener() {
      @Override
      public void notifyChannelEvent(ChannelEvent event) {
        switch(event.getType()) {
          case TRANSPORT_CLOSED_EVENT:
          case TRANSPORT_RECONNECTION_REJECTED_EVENT:
            shutdown();
        }
      }
    });

    final ClientIDLoggerProvider cidLoggerProvider = new ClientIDLoggerProvider(clientChannel::getClientID);
    this.communicationStageManager.setLoggerProvider(cidLoggerProvider);

    DSO_LOGGER.debug("Created channel.");


    clientEntityManager = this.clientBuilder.createClientEntityManager(clientChannel, this.communicationStageManager);
    RequestReceiveHandler singleMessageReceiver = new RequestReceiveHandler(clientEntityManager);
    MultiRequestReceiveHandler mutil = new MultiRequestReceiveHandler(clientEntityManager);
    Stage<VoltronEntityMultiResponse> multiResponseStage = this.communicationStageManager.createStage(ClientConfigurationContext.VOLTRON_ENTITY_MULTI_RESPONSE_STAGE, VoltronEntityMultiResponse.class, mutil, 1, maxSize);
    clientChannel.addAttachment("ChannelStats", (PrettyPrintable)() -> {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("messageHandler", mutil.getStateMap());
        return map;
      }, true);
    
    final ProductInfo pInfo = ProductInfo.getInstance(getClass().getClassLoader());
    
    ClientHandshakeMessageFactory chmf = (u, n, c)->{
      ClientMessageChannel cmc = getClientMessageChannel();
      if (cmc != null) {
        final ClientHandshakeMessage rv = (ClientHandshakeMessage)cmc.createMessage(TCMessageType.CLIENT_HANDSHAKE_MESSAGE);
        rv.setClientVersion(c);
        rv.setClientPID(getPID());
        rv.setUUID(u);
        rv.setName(n);
        return rv;
      } else {
        return null;
      }
    };
    
    this.clientHandshakeManager = this.clientBuilder
        .createClientHandshakeManager(new ClientIDLogger(clientChannel, LoggerFactory
                                          .getLogger(ClientHandshakeManagerImpl.class)), chmf, sessionManager,
                                          this.uuid, this.name, pInfo.version(), clientEntityManager);

    ClientChannelEventController.connectChannelEventListener(clientChannel, clientHandshakeManager);
    final ClientConfigurationContext cc = new ClientConfigurationContext(clientChannel.getClientID().toString(), this.communicationStageManager);
    // DO NOT create any stages after this call
    
    String[] exclusion = clientChannel.getProductID() == ProductID.DIAGNOSTIC || !isAsync ? 
      new String[] {
        ClientConfigurationContext.VOLTRON_ENTITY_MULTI_RESPONSE_STAGE
      } 
              :
      new String[] {
      };

    this.communicationStageManager.startAll(cc, Collections.<PostInit> emptyList(), exclusion);

    EventHandler<ClientHandshakeResponse> handshake = new ClientCoordinationHandler(this.clientHandshakeManager);

    initChannelMessageRouter(messageRouter, EventHandler.directSink(handshake), isAsync ? multiResponseStage.getSink() : EventHandler.directSink(mutil),
            clientEntityManager, singleMessageReceiver);

    return clientChannel;
  }

  private boolean directConnect(ClientMessageChannel clientChannel) {
    try {
      clientChannel.open(serverAddresses);
      waitForHandshake(clientChannel);
      connectionMade();
      return true;
    } catch (CommStackMismatchException |
            MaxConnectionsExceededException |
            TCTimeoutException tt) {
      DSO_LOGGER.error(tt.getMessage());
      throw new IllegalStateException(tt);
    } catch (IOException io) {
      DSO_LOGGER.debug("connection error", io);
      return false;
    }
  }

  private void connectionSequence(ClientMessageChannel clientChannel) {
    try {
      openChannel(clientChannel);
      waitForHandshake(clientChannel);
      connectionMade();
    } catch (RuntimeException | InterruptedException runtime) {
      exceptionMade.set(runtime);
    }
  }

  private void connectionMade() {
    connectionMade.attemptSet();
  }

  public void addShutdownHook(Runnable r) {
    this.shutdownManager.registerBeforeShutdownHook(r);
  }

  private boolean waitForConnection(long timeout, TimeUnit units) throws InterruptedException {
    if (!connectionThread.isSet()) {
      throw new IllegalStateException("not started");
    }
    connectionThread.get().join(units.toMillis(timeout));

    if (exceptionMade.isSet()) {
      Exception exp = exceptionMade.get();
      throw new RuntimeException(exp);
    }
    if (!connectionMade.isSet()) {
      shutdown();
      return false;
    } else {
      return true;
    }
  }

  private void openChannel(ClientMessageChannel channel) throws InterruptedException {
    while (!clientStopped.isSet()) {
      try {
        DSO_LOGGER.debug("Trying to open channel....");
        channel.open(serverAddresses);
        DSO_LOGGER.debug("Channel open");
        break;
      } catch (final TCTimeoutException tcte) {
        DSO_LOGGER.info("Unable to connect to server/s {} ...sleeping for 5 sec.", serverAddresses);
        DSO_LOGGER.debug("Timeout connecting to server/s: {} {}", serverAddresses, tcte.getMessage());
        synchronized(clientStopped) {
          clientStopped.wait(5000);
        }
      } catch (final ConnectException e) {
        DSO_LOGGER.info("Unable to connect to server/s {} ...sleeping for 5 sec.", serverAddresses);
        DSO_LOGGER.debug("Connection refused from server/s: {} {}", serverAddresses, e.getMessage());
        synchronized(clientStopped) {
          clientStopped.wait(5000);
        }
      } catch (final MaxConnectionsExceededException e) {
        DSO_LOGGER.error(e.getMessage());
        throw new IllegalStateException(e.getMessage(), e);
      } catch (final CommStackMismatchException e) {
        DSO_LOGGER.error(e.getMessage());
        throw new IllegalStateException(e.getMessage(), e);
      } catch (TransportHandshakeException handshake) {
        DSO_LOGGER.error(handshake.getMessage());
        throw new IllegalStateException(handshake.getMessage(), handshake);
      } catch (final IOException ioe) {
        DSO_LOGGER.info("Unable to connect to server/s {} ...sleeping for 5 sec.", serverAddresses);
        DSO_LOGGER.debug("IOException connecting to server/s: {} {}", serverAddresses, ioe.getMessage());
        synchronized(clientStopped) {
          clientStopped.wait(5000);
        }
      }
    }
  }

  private void waitForHandshake(ClientMessageChannel channel) {
    this.clientHandshakeManager.waitForHandshake();
    ClientMessageChannel cmc = this.getClientMessageChannel();
    if (cmc != null) {
      final TCSocketAddress remoteAddress = cmc.getRemoteAddress();
      final String infoMsg = "Connection successfully established to server at " + remoteAddress;
      if (!channel.getProductID().isInternal() && channel.isConnected()) {
        DSO_LOGGER.info(infoMsg);
      }
    }
  }

  private Map<TCMessageType, Class<? extends TCMessage>> getMessageTypeClassMapping() {
    final Map<TCMessageType, Class<? extends TCMessage>> messageTypeClassMapping = new HashMap<TCMessageType, Class<? extends TCMessage>>();

    messageTypeClassMapping.put(TCMessageType.CLIENT_HANDSHAKE_MESSAGE, ClientHandshakeMessageImpl.class);
    messageTypeClassMapping.put(TCMessageType.CLIENT_HANDSHAKE_ACK_MESSAGE, ClientHandshakeAckMessageImpl.class);
    messageTypeClassMapping
        .put(TCMessageType.CLIENT_HANDSHAKE_REFUSED_MESSAGE, ClientHandshakeRefusedMessageImpl.class);
    messageTypeClassMapping.put(TCMessageType.CLUSTER_MEMBERSHIP_EVENT_MESSAGE, ClusterMembershipMessage.class);
    messageTypeClassMapping.put(TCMessageType.VOLTRON_ENTITY_MESSAGE, NetworkVoltronEntityMessageImpl.class);
    messageTypeClassMapping.put(TCMessageType.VOLTRON_ENTITY_RECEIVED_RESPONSE, VoltronEntityReceivedResponseImpl.class);
    messageTypeClassMapping.put(TCMessageType.VOLTRON_ENTITY_COMPLETED_RESPONSE, VoltronEntityAppliedResponseImpl.class);
    messageTypeClassMapping.put(TCMessageType.VOLTRON_ENTITY_RETIRED_RESPONSE, VoltronEntityRetiredResponseImpl.class);
    messageTypeClassMapping.put(TCMessageType.VOLTRON_ENTITY_MULTI_RESPONSE, LinearVoltronEntityMultiResponse.class);
    messageTypeClassMapping.put(TCMessageType.DIAGNOSTIC_REQUEST, DiagnosticMessageImpl.class);
    messageTypeClassMapping.put(TCMessageType.DIAGNOSTIC_RESPONSE, DiagnosticResponseImpl.class);
    return messageTypeClassMapping;
  }

  private void initChannelMessageRouter(TCMessageRouter messageRouter, Sink<ClientHandshakeResponse> ack, 
                                         Sink<VoltronEntityMultiResponse> multiSink, ClientEntityManager cem, RequestReceiveHandler single) {
    Function<VoltronEntityResponse, VoltronEntityMultiResponse> multiConverter = (response)-> {
      return new ReplayVoltronEntityMultiResponse() {
        @Override
        public int replay(VoltronEntityMultiResponse.ReplayReceiver receiver) {
          try {
            single.handleEvent(response);
            return 1;
          } catch (EventHandlerException ee) {
            throw new RuntimeException(ee);
          }
        }
      };
    };
    messageRouter.routeMessageType(TCMessageType.CLIENT_HANDSHAKE_ACK_MESSAGE, new TCMessageHydrateSink<>(ack));
    messageRouter.routeMessageType(TCMessageType.CLIENT_HANDSHAKE_REFUSED_MESSAGE, new TCMessageHydrateSink<>(ack));
    messageRouter.routeMessageType(TCMessageType.CLIENT_HANDSHAKE_REDIRECT_MESSAGE, new TCMessageHydrateSink<>(ack));
    messageRouter.routeMessageType(TCMessageType.CLUSTER_MEMBERSHIP_EVENT_MESSAGE, new TCMessageHydrateSink<>((context) -> {/* black hole for compatibility */}));
    messageRouter.routeMessageType(TCMessageType.VOLTRON_ENTITY_RECEIVED_RESPONSE, new TCMessageHydrateAndConvertSink<>(multiSink, multiConverter));
    messageRouter.routeMessageType(TCMessageType.VOLTRON_ENTITY_COMPLETED_RESPONSE, new TCMessageHydrateAndConvertSink<>(multiSink, multiConverter));
    messageRouter.routeMessageType(TCMessageType.VOLTRON_ENTITY_RETIRED_RESPONSE, new TCMessageHydrateAndConvertSink<>(multiSink, multiConverter));
    messageRouter.routeMessageType(TCMessageType.VOLTRON_ENTITY_MULTI_RESPONSE, new TCMessageHydrateSink<>(multiSink));
    messageRouter.routeMessageType(TCMessageType.DIAGNOSTIC_RESPONSE, new TCMessageHydrateAndConvertSink<DiagnosticResponse, Void>(null, (r)-> {
      cem.complete(r.getTransactionID(), r.getResponse());
      return null;
    }));
    DSO_LOGGER.debug("Added message routing types.");
  }

  public ClientEntityManager getEntityManager() {
    return this.clientEntityManager;
  }

  public String getClientState() {
    PrettyPrinter printer = new MapListPrettyPrint();
    this.communicationStageManager.prettyPrint(printer);
    this.clientEntityManager.prettyPrint(printer);
    return printer.toString();
  }

  public void dump() {
    DSO_LOGGER.info(getClientState());
  }

  void shutdownResources() {
    final Logger logger = DSO_LOGGER;

    if (this.counterManager != null) {
      try {
        this.counterManager.shutdown();
      } catch (final Throwable t) {
        logger.error("error shutting down counter manager", t);
      } finally {
        this.counterManager = null;
      }
    }

    if (this.clientHandshakeManager != null) {
      this.clientHandshakeManager.shutdown();
    }

    ClientMessageChannel clientChannel = this.getClientMessageChannel();
    if (clientChannel != null) {
      try {
        clientChannel.close();
      } catch (final Throwable t) {
        logger.error("Error closing channel", t);
      } finally {

      }
    }

    if (this.communicationsManager != null) {
      try {
        this.communicationsManager.shutdown();
      } catch (final Throwable t) {
        logger.error("Error shutting down communications manager", t);
      } finally {
        this.communicationsManager = null;
      }
    }
    
    if (this.connectionManager != null) {
      try {
        this.connectionManager.shutdown();
      } catch (final Throwable t) {
        logger.error("Error shutting down connection manager", t);
      } finally {
        this.connectionManager = null;
      }
    }

    try {
      this.communicationStageManager.stopAll();
    } catch (final Throwable t) {
      logger.error("Error stopping stage manager", t);
    }
    
    CommonShutDownHook.shutdown();

    if (this.threadGroup != null) {
      final long timeout = TCPropertiesImpl.getProperties()
                             .getLong(TCPropertiesConsts.L1_SHUTDOWN_THREADGROUP_GRACETIME);
      SetOnceFlag interrupted = new SetOnceFlag();
      try {
        if (!threadGroup.retire(timeout, e->interrupted.attemptSet())) {
            logger.warn("Timed out waiting for TC thread group threads to die for connection " + name + "/" + uuid + " - probable shutdown memory leak\n"
                     + " in thread group " + this.threadGroup);
            threadGroup.printLiveThreads(logger::warn);
            ThreadUtil.executeInThread(threadGroup.getParent(), ()->{
            if (!threadGroup.retire(timeout, e->interrupted.attemptSet())) {
              threadGroup.interrupt();
            }
          }, name + " - Connection Reaper", true);
        }
      } catch (final Throwable t) {
        logger.error("Error destroying TC thread group", t);
      } finally {
        if (interrupted.isSet()) {
          Thread.currentThread().interrupt();
        }
      }
    }

    if (TCPropertiesImpl.getProperties().getBoolean(TCPropertiesConsts.L1_SHUTDOWN_FORCE_FINALIZATION)) System
        .runFinalization();
  }

  public void shutdown() {
    if (connectionThread.isSet()) {
      connectionThread.get().interrupt();
    }
    if (clientStopped.attemptSet()) {
      synchronized (clientStopped) {
        clientStopped.notifyAll();
      }
      ClientMessageChannel clientChannel = this.getClientMessageChannel();
      if (clientChannel != null && !clientChannel.getProductID().isInternal() && clientChannel.isConnected()) {
        DSO_LOGGER.info("closing down Terracotta Connection channel=" + clientChannel.getChannelID() + " client=" + clientChannel.getClientID());
      }
      this.shutdownManager.execute();
    }
  }
  
  private int getPID() {
    String vmName = ManagementFactory.getRuntimeMXBean().getName();
    int index = vmName.indexOf('@');

    if (index < 0) { throw new RuntimeException("unexpected format: " + vmName); }

    return Integer.parseInt(vmName.substring(0, index));
  }
  
  private synchronized ClientMessageChannel getClientMessageChannel() {
    return this.channel;
  }
}
