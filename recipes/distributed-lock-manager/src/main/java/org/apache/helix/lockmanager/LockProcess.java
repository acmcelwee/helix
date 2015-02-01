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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Joiner;
import org.apache.helix.*;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;

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

    addLiveInstanceChangeListener();
    addMessageListener();
    addInstanceConfigChangeListener();

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

  private void addLiveInstanceChangeListener() throws Exception {
//    participantManager.addCurrentStateChangeListener(new CurrentStateChangeListener() {
//      @Override
//      public void onStateChange(String instanceName, List<CurrentState> statesInfo, NotificationContext changeContext) {
//        helixAdmin.rebalance(clusterName, resourceName, 1);
//
//        Function<CurrentState, String> mapper = new Function<CurrentState, String>() {
//          @Override
//          public String apply(CurrentState currentState) {
//            return Joiner.on(',').withKeyValueSeparator("=").join(currentState.getPartitionStateMap());
//          }
//        };
//        System.out.println(String.format(
//            "CurrentStateChangeListener: instanceName - %s  -- statesInfo - %s  --  changeContext - %s",
//            instanceName,
//            Joiner.on(',').join(statesInfo.stream().map(mapper).toArray()),
//            "foo"
//        ));
//      }
//    }, instanceName, participantManager.getSessionId());
    participantManager.addLiveInstanceChangeListener(new LiveInstanceChangeListener() {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
        // Hmm, so much for "auto rebalance"
        helixAdmin.rebalance(clusterName, resourceName, 1);

        Function<LiveInstance, String> mapper = new Function<LiveInstance, String>() {
          @Override
          public String apply(LiveInstance liveInstance) {
            return String.format("%s - %s", liveInstance.getParticipantId(), liveInstance.getInstanceName());
          }
        };
        System.out.println(String.format(
            "LiveInstanceChangeListener: liveInstances - %s   --  changeContext - %s",
            Joiner.on(',').join(liveInstances.stream().map(mapper).toArray()),
            notificationContextToString(changeContext)
        ));
      }
    });
  }

  private void addInstanceConfigChangeListener() throws Exception {
    participantManager.addInstanceConfigChangeListener(new InstanceConfigChangeListener() {
      @Override
      public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {

      }
    });
  }

  private void addMessageListener() throws Exception {
    participantManager.addMessageListener(new MessageListener() {
      @Override
      public void onMessage(String instanceName, List<Message> messages, NotificationContext changeContext) {

        Function<Message, String> mapper = new Function<Message, String>() {
          @Override
          public String apply(Message message) {
            return "";
          }
        };
        System.out.println(String.format(
            "MessageListener: instanceName - %s  --  messages - %s  --  changeContext - %s",
            instanceName,
            Joiner.on(',').join(messages.stream().map(mapper).toArray()),
            notificationContextToString(changeContext)
        ));
      }
    }, instanceName);
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
      helixAdmin.addInstance(clusterName, instanceConfig);
    }
  }

  public void stop(InstanceConfig instanceConfig) {
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
    instanceConfig.setHostName("localhost");
    instanceConfig.setPort(Integer.toString(port));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down " + instanceName);
        lockProcess.stop(instanceConfig);
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
