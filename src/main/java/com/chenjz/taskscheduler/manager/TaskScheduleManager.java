package com.chenjz.taskscheduler.manager;

import com.google.common.collect.Maps;
import com.chenjz.taskscheduler.model.NodeTask;
import com.chenjz.taskscheduler.model.NodeTaskStatus;
import com.chenjz.taskscheduler.model.ParentTask;

import java.util.Map;
import java.util.UUID;

/**
 * Desc:
 * <p>
 * User: liulin ,Date: 2018/3/27 , Time: 22:15 <br/>
 * Email: liulin@cmss.chinamobile.com <br/>
 * To change this template use File | Settings | File Templates.
 */
public enum TaskScheduleManager {instance;

    /**
     * 任务调度器线程Map （每个ParentTask 对应一个Thread）
     */
    private Map<String, Thread> taskScheduleThreadMap = Maps.newConcurrentMap();

    /**
     * 当前所有 NodeTasks 会被当成一个整体进行调度（形成一个 有向无环图 tasks）
     *
     * @param nodeTasks      所有的tasks
     * @param statusListener 用于监听任务的状态
     */
    public ParentTask startNodeTasks(Map<String, NodeTask> nodeTasks, ITaskStatusListener statusListener) {
        Map<String, NodeTask> nodeTasksConcurrent = Maps.newConcurrentMap();
        nodeTasksConcurrent.putAll(nodeTasks);

        String parentTaskId = UUID.randomUUID().toString();
        for (NodeTask nodeTask : nodeTasksConcurrent.values()) {
            nodeTask.setParentId(parentTaskId);
        }

        ParentTask parentTask = ParentTask.builder().id(parentTaskId).nodeTasks(nodeTasksConcurrent)
                .taskStatusListener(statusListener).build();

        TaskScheduleManager.instance.startParentTask(parentTask);
        return parentTask;
    }

    /**
     * 当前所有 NodeTasks 会被当成一个整体进行调度（形成一个 有向无环图 tasks）
     *
     * @param nodeTasks 所有的tasks
     */
    public void startNodeTasks(Map<String, NodeTask> nodeTasks) {
        startNodeTasks(nodeTasks, null);
    }

    /**
     * 开始ParentTask调度
     * <p>
     *
     * @param parentTask
     */
    private void startParentTask(ParentTask parentTask) {
        startParentTask(parentTask, false);
    }

    /**
     * 取消 ParentTask 调度
     *
     * @param parentTaskId
     */
    public void cancelParentTskSchedule(String parentTaskId) {
        synchronized (taskScheduleThreadMap) {// 可能两个NodeTask同时失败，同时取消
            if (taskScheduleThreadMap.get(parentTaskId) != null) {
                taskScheduleThreadMap.get(parentTaskId).interrupt();
            }
        }
    }

    public void reRunNodeTask(ParentTask parentTask) {
        parentTask.setParentStatus(NodeTaskStatus.init);
        parentTask.setTaskFail(false);
        taskScheduleThreadMap.remove(parentTask.getId());
        TaskScheduleManager.instance.startParentTask(parentTask, true);
    }

    private void startParentTask(ParentTask parentTask, boolean skipSucceed) {
        if (taskScheduleThreadMap.get(parentTask.getId()) == null) {
            synchronized (taskScheduleThreadMap) {
                TaskManager.instance.addTask(parentTask);
                Thread scheduleThread = new Thread(() -> {
                    TaskExecutor.instance.startTaskSchedule(parentTask.getId(), skipSucceed);
                });
                taskScheduleThreadMap.put(parentTask.getId(), scheduleThread);
                scheduleThread.start();
            }
        } else {
            throw new RuntimeException("Duplicate start parentTask:" + parentTask.getId());
        }
    }}
