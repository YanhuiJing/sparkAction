package com.spark.electricStore.dao.factory;

import com.spark.electricStore.dao.IAdBlacklistDAO;
import com.spark.electricStore.dao.IAdClickTrendDAO;
import com.spark.electricStore.dao.IAdProvinceTop3DAO;
import com.spark.electricStore.dao.IAdStatDAO;
import com.spark.electricStore.dao.IAdUserClickCountDAO;
import com.spark.electricStore.dao.IAreaTop3ProductDAO;
import com.spark.electricStore.dao.IPageSplitConvertRateDAO;
import com.spark.electricStore.dao.ISessionAggrStatDAO;
import com.spark.electricStore.dao.ISessionDetailDAO;
import com.spark.electricStore.dao.ISessionRandomExtractDAO;
import com.spark.electricStore.dao.ITaskDAO;
import com.spark.electricStore.dao.ITop10CategoryDAO;
import com.spark.electricStore.dao.ITop10SessionDAO;
import com.spark.electricStore.dao.impl.AdBlacklistDAOImpl;
import com.spark.electricStore.dao.impl.AdClickTrendDAOImpl;
import com.spark.electricStore.dao.impl.AdProvinceTop3DAOImpl;
import com.spark.electricStore.dao.impl.AdStatDAOImpl;
import com.spark.electricStore.dao.impl.AdUserClickCountDAOImpl;
import com.spark.electricStore.dao.impl.AreaTop3ProductDAOImpl;
import com.spark.electricStore.dao.impl.PageSplitConvertRateDAOImpl;
import com.spark.electricStore.dao.impl.SessionAggrStatDAOImpl;
import com.spark.electricStore.dao.impl.SessionDetailDAOImpl;
import com.spark.electricStore.dao.impl.SessionRandomExtractDAOImpl;
import com.spark.electricStore.dao.impl.TaskDAOImpl;
import com.spark.electricStore.dao.impl.Top10CategoryDAOImpl;
import com.spark.electricStore.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
