package com.chenjz.taskscheduler.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * <p>
 * User: liulin ,Date: 2018/3/27 , Time: 12:32 <br/>
 * Email: liulin@cmss.chinamobile.com <br/>
 * To change this template use File | Settings | File Templates.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NodeTaskResult {

    private String id;          // 任务唯一标识

    private Object result;     // 子任务执行结果
}
