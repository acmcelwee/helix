package org.apache.helix.lockmanager;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

import java.util.List;
import java.util.function.Function;

public class LockProcess {
  private String clusterName;
  private String resourceName;
  private String zkAddress;
  private String instanceName;
  private HelixManager participantManager;
  private boolean startController;
  private ZKHelixAdmin helixAdmin;
  private HelixManager controllerManager;

  LockProcess(String clusterName, String resourceName, String zkAddress, String instanceName, boolean startController) {
    this.clusterName = clusterName;
    this.resourceName = resourceName;
    this.zkAddress = zkAddress;
    this.instanceName = instanceName;
    this.startController = startController;
    this.helixAdmin = new ZKHelixAdmin(zkAddress);
  }

  public void start(InstanceConfig instanceConfig) throws Exception {
    System.out.println("STARTING " + instanceConfig.getInstanceName());
    configureInstance(instanceConfig);

    if (startController) {
      startController();
    }

    participantManager =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkAddress);

    LockFactory lockFactory = createLockFactory();
    participantManager.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("OnlineOffline"), lockFactory);
    participantManager.connect();

    System.out.println("STARTED " + instanceName);

  }

  private LockFactory createLockFactory() {
    Function<Integer, Void> lockAcquiredHandler = new Function<Integer, Void>() {
      @Override
      public Void apply(Integer integer) {
        System.out.println("Acquired partition " + integer);
        return null;
      }
    };
    Function<Integer, Void> lockReleasedHandler = new Function<Integer, Void>() {
      @Override
      public Void apply(Integer integer) {
        System.out.println("Releasing partition " + integer);
        return null;
      }
    };

    return new LockFactory(lockAcquiredHandler, lockReleasedHandler);
  }

  private void startController() {
    controllerManager =
        HelixControllerMain.startHelixController(zkAddress, clusterName, "controller",
            HelixControllerMain.STANDALONE);
  }

  /**
   * Configure the instance, the configuration of each node is available to
   * other nodes.
   * @param instanceConfig -
   */
  private void configureInstance(InstanceConfig instanceConfig) {
    List<String> instancesInCluster = helixAdmin.getInstancesInCluster(clusterName);
    System.out.println("InstancesInCluster: " + instancesInCluster);
    if (instancesInCluster == null || !instancesInCluster.contains(instanceConfig.getInstanceName())) {
      // TODO - Need to update the ZKHelixAdmin class to optionally create ephemeral nodes
      helixAdmin.addInstance(clusterName, instanceConfig);
    }
  }

  public void stop() {
    if (participantManager != null) {
      participantManager.disconnect();
    }

    if (controllerManager != null) {
      controllerManager.disconnect();
    }
  }

  public static void main(String[] args) throws Exception {
    String zkAddress = "localhost:2199";

    final String clusterName = "lock-manager-demo";
    final String lockGroupName = "lock-group";

    int port = Integer.valueOf(args[0]);

    // Give a unique id to each process, most commonly used format hostname_port
    final String instanceName = String.format("localhost_%s", port);
      final LockProcess lockProcess =
        new LockProcess(clusterName, lockGroupName, zkAddress, instanceName, port == 12000);
    final InstanceConfig instanceConfig = new InstanceConfig(instanceName);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down " + instanceName);
        lockProcess.stop();
      }
    });
    lockProcess.start(instanceConfig);
    Thread.currentThread().join();
  }

  public static String notificationContextToString(NotificationContext notificationContext) {
    return String.format(
        "[changeContext: eventName - %s, pathChanged - %s, type - %s]",
        notificationContext.getEventName(),
        notificationContext.getPathChanged(),
        notificationContext.getType().toString());
  }
}
