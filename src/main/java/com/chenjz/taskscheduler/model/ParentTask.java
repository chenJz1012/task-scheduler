package com.chenjz.taskscheduler.model;

import com.chenjz.taskscheduler.manager.ITaskStatusListener;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.Data;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Desc:
 * <p>
 * User: liulin ,Date: 2018/3/27 , Time: 17:43 <br/>
 * Email: liulin@cmss.chinamobile.com <br/>
 * To change this template use File | Settings | File Templates.
 */
@Data
@Builder
public class ParentTask {

    private String id;

    private Map<String, NodeTask> nodeTasks = Maps.newConcurrentMap();

    private AtomicInteger nodeTskSucceedCount;  //成功结束的NodeTask个数

    private volatile boolean isTaskFail = false;

    private NodeTaskStatus parentStatus = NodeTaskStatus.init;

    private ITaskStatusListener taskStatusListener;

    public void setParentStatus(NodeTaskStatus parentStatus) {
        this.parentStatus = parentStatus;
    }

    public void validate() {
        if (Strings.isNullOrEmpty(id) || CollectionUtils.isEmpty(nodeTasks)) {
            throw new RuntimeException("ParentTask validate fail.");
        }
    }

    public NodeTask getNodeTask(String nodeTaskId) {
        return nodeTasks.get(nodeTaskId);
    }

    public int nodeTaskSuccess() {
        if (nodeTskSucceedCount == null) {
            synchronized (this) {
                if (nodeTskSucceedCount == null) {
                    nodeTskSucceedCount = new AtomicInteger(0);
                }
            }
        }

        return nodeTskSucceedCount.addAndGet(1);
    }

    public void nodeTaskFail() {
        this.setTaskFail(true);
    }

    public boolean isParentTaskFailOrFinish() {
        return isParentTaskFinish() || isParentTaskFail();
    }

    public boolean isParentTaskFinish() {
        if (nodeTskSucceedCount.get() == nodeTasks.size()) {
            return true;
        }
        return false;
    }

    public boolean isParentTaskFail() {
        if (isTaskFail) {
            return true;
        }
        return false;
    }

    public double progress() {
        validate();
        return (double) nodeTskSucceedCount.get() / nodeTasks.size();
    }

}
