package com.chenjz.taskscheduler.demo;

import com.chenjz.taskscheduler.model.NodeTask;
import lombok.Data;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Desc:
 * <p>
 * User: liulin ,Date: 2018/3/27 , Time: 15:42 <br/>
 * Email: liulin@cmss.chinamobile.com <br/>
 * To change this template use File | Settings | File Templates.
 */
@Data
public class MyNodeTask extends NodeTask<String> {

    private long runTime;

    public MyNodeTask(long runTime, String id, Set<String> dependencies) {
        super(id, dependencies);
        this.runTime = runTime;
    }

    @Override
    public String doNodeTaskWork() throws Exception {
        System.out.println("Begin to run MyNodeTask【" + this.getId() + "】");
        if (this.getId().endsWith("F") && !"retry".equalsIgnoreCase(this.getType())) {
            throw new RuntimeException("NodeTask F exception");
        }
        TimeUnit.MILLISECONDS.sleep(runTime);
        return "[finish] MyNodeTask( " + this.getId() + " ) exec finish";
    }
}
