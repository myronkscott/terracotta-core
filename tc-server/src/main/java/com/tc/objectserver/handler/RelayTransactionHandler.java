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
package com.tc.objectserver.handler;

import com.tc.async.api.AbstractEventHandler;
import com.tc.async.api.ConfigurationContext;
import com.tc.async.api.EventHandler;
import com.tc.async.api.EventHandlerException;
import com.tc.async.api.Stage;
import com.tc.exception.ServerException;
import com.tc.objectserver.entity.MessagePayload;
import com.tc.l2.msg.ReplicationMessage;
import com.tc.l2.msg.ReplicationResultCode;
import com.tc.l2.msg.SyncReplicationActivity;
import com.tc.l2.state.StateManager;
import com.tc.net.NodeID;
import com.tc.net.ServerID;
import com.tc.net.groups.AbstractGroupMessage;
import com.tc.net.groups.GroupManager;
import com.tc.objectserver.core.api.ServerConfigurationContext;
import com.tc.util.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RelayTransactionHandler {
  private static final Logger PLOGGER = LoggerFactory.getLogger(MessagePayload.class);
  private static final Logger LOGGER = LoggerFactory.getLogger(RelayTransactionHandler.class);

  private final PassiveAckSender ackSender;
  private volatile long currentSequence = 0;
  private StateManager stateMgr;
  private NodeID endTarget;
  
  

  public long getCurrentSequence() {
    return currentSequence;
  }
  
  public RelayTransactionHandler(Stage<Runnable> sendToActive, GroupManager<AbstractGroupMessage> groupManager) {
    this.ackSender = new PassiveAckSender(groupManager, m->true, sendToActive.getSink());
  }

  private final EventHandler<ReplicationMessage> eventHorizon = new AbstractEventHandler<ReplicationMessage>() {
    @Override
    public void handleEvent(ReplicationMessage message) throws EventHandlerException {
      try {
        currentSequence = message.getSequenceID();
        processMessage(message);
      } catch (Throwable t) {
        // We don't expect to see an exception executing a replicated message.
        // TODO:  Find a better way to handle this error.
        throw Assert.failure("Unexpected exception executing replicated message", t);
      }
    }

    @Override
    protected void initialize(ConfigurationContext context) {
      ServerConfigurationContext scxt = (ServerConfigurationContext)context;
      stateMgr = scxt.getL2Coordinator().getStateManager();
    } 
  };
  
  public boolean registerRelayConsumer(NodeID node) {
    NodeID active = stateMgr.getActiveNodeID();
    LOGGER.info("remote node connected for duplication {}", node);
    if (!active.isNull()) {
      ackSender.requestPassiveSync(stateMgr.getActiveNodeID());
      endTarget = node;
      return true;
    } else {
      return false;
    }
  }

  public EventHandler<ReplicationMessage> getEventHandler() {
    return eventHorizon;
  }

  private void processMessage(ReplicationMessage rep) throws ServerException {
    if (PLOGGER.isDebugEnabled()) {
      PLOGGER.debug("RECEIVED:" + rep.getDebugId());
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("BATCH:" + rep.getSequenceID());
    }
    ServerID activeSender = rep.messageFrom();
    for (SyncReplicationActivity activity : rep.getActivities()) {
      LOGGER.info(activity.toString());
      ackSender.acknowledge(activeSender, activity, ReplicationResultCode.NONE);
    }
  }
}
