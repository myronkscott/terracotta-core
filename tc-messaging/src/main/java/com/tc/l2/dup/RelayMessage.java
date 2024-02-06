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
package com.tc.l2.dup;

import com.tc.io.TCByteBufferInput;
import com.tc.io.TCByteBufferOutput;
import com.tc.l2.msg.ReplicationMessage;
import com.tc.net.groups.AbstractGroupMessage;
import java.io.IOException;
import java.util.function.Consumer;

public class RelayMessage extends AbstractGroupMessage {

  public static final int        START_SYNC       = 0x01;
  public static final int        RELAY_BATCH       = 0x02;



  // To make serialization happy
  public RelayMessage() {
    super(-1);
  }

  public RelayMessage(int type) {
    super(type);
  }

  @Override
  protected void basicDeserializeFrom(TCByteBufferInput in) throws IOException {
    switch (getType()) {
      case START_SYNC:
      case RELAY_BATCH:
    }
  }

  @Override
  protected void basicSerializeTo(TCByteBufferOutput out) {
    
  }
  
  public static AbstractGroupMessage createStartSync() {
    return new RelayMessage(START_SYNC);
  }
  
  public void unwindBatch(Consumer<ReplicationMessage> next) {
    
  }
  
}