package com.chenjz.taskscheduler.demo;

import com.chenjz.taskscheduler.manager.TaskScheduleManager;
import com.chenjz.taskscheduler.model.NodeTask;
import com.chenjz.taskscheduler.model.ParentTask;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Map;

public class TaskScheduleManagerTests {

    @Test
    public void testSingleTypeTask() throws InterruptedException {
        NodeTask nodeTaskA = new MyNodeTask(1_000, "nodeA", null);
        NodeTask nodeTaskB = new MyNodeTask(1_000, "nodeB", null);
        NodeTask nodeTaskC = new MyNodeTask(1_000, "nodeC", Sets.newHashSet(nodeTaskA.getId()));
        NodeTask nodeTaskD = new MyNodeTask(1_000, "nodeD", Sets.newHashSet(nodeTaskB.getId()));
        NodeTask nodeTaskE = new MyNodeTask(1_000, "nodeE", Sets.newHashSet(nodeTaskC.getId(), nodeTaskD.getId()));
        NodeTask nodeTaskF = new MyNodeTask(1_000, "nodeF", Sets.newHashSet(nodeTaskE.getId()));
        NodeTask nodeTaskG = new MyNodeTask(1_000, "nodeG", Sets.newHashSet(nodeTaskE.getId()));
        NodeTask nodeTaskH = new MyNodeTask(1_000, "nodeH", Sets.newHashSet(nodeTaskF.getId()));

        Map<String, NodeTask> nodeTaskMap = Maps.newConcurrentMap();
        nodeTaskMap.put(nodeTaskA.getId(), nodeTaskA);
        nodeTaskMap.put(nodeTaskB.getId(), nodeTaskB);
        nodeTaskMap.put(nodeTaskC.getId(), nodeTaskC);
        nodeTaskMap.put(nodeTaskD.getId(), nodeTaskD);
        nodeTaskMap.put(nodeTaskE.getId(), nodeTaskE);
        nodeTaskMap.put(nodeTaskF.getId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getId(), nodeTaskG);
        nodeTaskMap.put(nodeTaskH.getId(), nodeTaskH);

        /////////////////////////
        ParentTask parentTask = TaskScheduleManager.instance.startNodeTasks(nodeTaskMap, new MyTaskStatusListener());
        while (true) {
            if (parentTask.isParentTaskFail()) {
                Thread.sleep(2000);
                TaskScheduleManager.instance.reRunNodeTask(parentTask);
                break;
            }
        }
        while (true) ;
    }

}
