/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.testing.master;

import org.terracotta.testing.common.Assert;
import org.terracotta.testing.logging.VerboseManager;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Handles the description, installation, and start-up of a stripe of servers in a cluster.
 */
public class StripeInstaller {
  private final StateInterlock interlock;
  private final ITestStateManager stateManager;
  private final VerboseManager stripeVerboseManager;
  private final List<ServerInstance> serverProcesses = new ArrayList<>();
  private final boolean inline;
  private int heapInM = 32;
  private Properties serverProperties = new Properties();
  private boolean isBuilt;

  public StripeInstaller(StateInterlock interlock, ITestStateManager stateManager, boolean inline, VerboseManager stripeVerboseManager) {
    this.interlock = interlock;
    this.stateManager = stateManager;
    this.stripeVerboseManager = stripeVerboseManager;
    this.inline = inline;
  }

  public void installNewServer(String serverName, Path server, Path workingDir, int debugPort, OutputStream out, String[] cmd) throws IOException {
    // Our implementation installs all servers before starting any (just an internal consistency check).
    Assert.assertFalse(this.isBuilt);
    // server install is now at the stripe level to use less disk
    Files.createDirectories(workingDir);
    // Create the object representing this single installation and add it to the list for this stripe.
    VerboseManager serverVerboseManager = this.stripeVerboseManager.createComponentManager("[" + serverName + "]");
    ServerInstance serverProcess = inline ?
      new ServerProcess(this.interlock, this.stateManager, serverVerboseManager, serverName,
        server, workingDir, heapInM, debugPort, serverProperties, out, cmd)
            :
      new InlineServer(this.interlock, this.stateManager, serverVerboseManager, serverName,
        server, workingDir, serverProperties, out, cmd);    
    serverProcesses.add(serverProcess);
  }

  public void startServers() {
    Assert.assertFalse(this.isBuilt);
    for (ServerInstance newProcess : serverProcesses) {
      try {
        // Note that starting the process will put it into the interlock and the server will notify it of state changes.
        newProcess.start();
      } catch (IOException e) {
        Assert.unexpected(e);
      }
    }
    this.isBuilt = true;
  }
}
