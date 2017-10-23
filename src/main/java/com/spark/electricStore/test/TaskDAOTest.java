package com.spark.electricStore.test;

import com.spark.electricStore.dao.ITaskDAO;
import com.spark.electricStore.dao.factory.DAOFactory;
import com.spark.electricStore.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());  
	}
	
}
