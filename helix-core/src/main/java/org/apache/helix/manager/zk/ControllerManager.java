package org.apache.helix.manager.zk;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.MessageListener;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.healthcheck.HealthStatsAggregationTask;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.ZKPathDataDumpTask;
import org.apache.helix.monitoring.mbeans.HelixStageLatencyMonitor;
import org.apache.helix.participant.DistClusterControllerElection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ControllerManager extends AbstractManager {
  private static Logger LOG = Logger.getLogger(ControllerManager.class);

  final GenericHelixController _controller = new GenericHelixController();
  
  // TODO merge into GenericHelixController
  private CallbackHandler _leaderElectionHandler = null;

  /**
   * status dump timer-task
   *
   */
  static class StatusDumpTask extends HelixTimerTask {
    Timer _timer = null;
    final ZkClient zkclient;
    final AbstractManager helixController;
    
    public StatusDumpTask(ZkClient zkclient, AbstractManager helixController) {
      this.zkclient = zkclient;
      this.helixController = helixController;
    }
    
    @Override
    public void start() {
      long initialDelay = 30 * 60 * 1000;
      long period = 120 * 60 * 1000;
      int timeThresholdNoChange = 180 * 60 * 1000;

      if (_timer == null)
      {
        _timer = new Timer(true);
        _timer.scheduleAtFixedRate(new ZKPathDataDumpTask(helixController,
                                                          zkclient,
                                                          timeThresholdNoChange),
                                   initialDelay,
                                   period);
      }
      
    }

    @Override
    public void stop() {
      if (_timer != null)
      {
        _timer.cancel();
        _timer = null;
      }      
    }

    @Override
    public void run() {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  public ControllerManager(String zkAddress, String clusterName, String instanceName) {
    super(zkAddress, clusterName, instanceName, InstanceType.CONTROLLER);
    
    _timerTasks.add(new HealthStatsAggregationTask(this));
    _timerTasks.add(new StatusDumpTask(_zkclient, this));
  }

  @Override
  protected List<HelixTimerTask> getControllerHelixTimerTasks() {
    return _timerTasks;
  }

  @Override
  public void handleNewSession() throws Exception {  
    waitUntilConnected();
    
    /**
     * reset all handlers, make sure cleanup completed for previous session
     * disconnect if fail to cleanup
     */    
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.reset();
    }
    // TODO reset user defined handlers only
    resetHandlers();

    /**
     * from here on, we are dealing with new session
     */
    
    if (_leaderElectionHandler != null) {
      _leaderElectionHandler.init();
    } else {
      _leaderElectionHandler = new CallbackHandler(this,
                                                   _zkclient,
                                                   _keyBuilder.controller(),
                                  new DistributedLeaderElection(this, _controller),
                                  new EventType[] { EventType.NodeChildrenChanged,
                                      EventType.NodeDeleted, EventType.NodeCreated },
                                  ChangeType.CONTROLLER);
    }
    
    /**
     * init user defined handlers only
     */
    List<CallbackHandler> userHandlers = new ArrayList<CallbackHandler>();
    for (CallbackHandler handler : _handlers) {
      Object listener = handler.getListener();
      if (!listener.equals(_messagingService.getExecutor())
          && !listener.equals(_controller)) {
        userHandlers.add(handler);
      }
    }
    initHandlers(userHandlers);

  }

  @Override
  void doDisconnect() {
    if (_leaderElectionHandler != null)
    {
      _leaderElectionHandler.reset();
    }
  }

  @Override
  public boolean isLeader() {
    if (!isConnected())
    {
      return false;
    }

    try {
      LiveInstance leader = _dataAccessor.getProperty(_keyBuilder.controllerLeader());
      if (leader != null)
      {
        String leaderName = leader.getInstanceName();
        String sessionId = leader.getSessionId();
        if (leaderName != null && leaderName.equals(_instanceName)
            && sessionId != null && sessionId.equals(_sessionId))
        {
          return true;
        }
      }
    } catch (Exception e) {
      // log
    }
    return false;  
  }

  /**
   * helix-controller uses a write-through cache for external-view
   * 
   */
  @Override
  BaseDataAccessor<ZNRecord> createBaseDataAccessor(ZkBaseDataAccessor<ZNRecord> baseDataAccessor) {
    String extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, _clusterName);
    return new ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor,
                                                Arrays.asList(extViewPath));

  }
  
}