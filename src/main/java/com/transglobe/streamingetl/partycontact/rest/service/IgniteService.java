package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Service;


public class IgniteService {
	static final Logger LOG = LoggerFactory.getLogger(IgniteService.class);

//	@Value("${ignite.config.file}")
	private String igniteConfigFile;

//	@Value("${partycontact.base.dir}")
	private String partyContactBaseDir;

//	@Value("${ignite.loaddata.script}")
	private String igniteLoaddataScript;
	
//	@Value("${ignite.bean.name}")
	private String igniteBeanName;

	private FileSystemXmlApplicationContext igniteCtx;

	private Process loadDataProcess;
	private ExecutorService loadDataExecutor;


	public void startIgnite() {

//		if (igniteCtx == null || !igniteCtx.isActive()) {
//			igniteCtx =new FileSystemXmlApplicationContext("file:"+igniteConfigFile);
//
//			//			try {
//			// Get ignite from Spring (note that local cluster node is already started).
//			Ignite ignite = (Ignite)igniteCtx.getBean(igniteBeanName);
//
//			LOG.info(">>> ignite started!!!");
//		} else {
//			LOG.info(">>> igniteCtx is still active. Cannot start");
//		}

	}

	public void stopIgnite() {

		if (igniteCtx == null || !igniteCtx.isActive()) {
			LOG.info(">>> igniteCtx is null or is not active. Cannot stop");

		} else {
			igniteCtx.close();

			LOG.info(">>> igniteCtx close.");
		}
	}
	public void stopLoadData() {
		if (loadDataProcess.isAlive()) {
			loadDataProcess.destroy();
			loadDataExecutor.shutdown();
			if (!loadDataExecutor.isTerminated()) {
				loadDataExecutor.shutdownNow();

				try {
					loadDataExecutor.awaitTermination(600, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
			LOG.info(">>> LoadData stopped");
		} else {
			LOG.info(">>> LoadData is NOT alive, cannot stop");
		}
	}
//	public void stopLoadData2() {
//		if (loadDataProcess2.isAlive()) {
//			loadDataProcess2.destroy();
//			loadDataExecutor2.shutdown();
//			if (!loadDataExecutor2.isTerminated()) {
//				loadDataExecutor2.shutdownNow();
//
//				try {
//					loadDataExecutor2.awaitTermination(600, TimeUnit.SECONDS);
//				} catch (InterruptedException e) {
//					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
//							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//				}
//
//			}
//			LOG.info(">>> LoadData2 stopped");
//		} else {
//			LOG.info(">>> LoadData2 is NOT alive, cannot stop");
//		}
//	}
	public void loadData() {

		try {
			if (loadDataProcess == null || !loadDataProcess.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				builder.command("sh", "-c", igniteLoaddataScript);

				builder.directory(new File(partyContactBaseDir));
				loadDataProcess = builder.start();

				loadDataExecutor = Executors.newSingleThreadExecutor();
				loadDataExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(loadDataProcess.getInputStream()));
						reader.lines().forEach(str -> LOG.info(str));
					}

				});

				int exitVal = loadDataProcess.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! Loaddata1 ");
				} else {
					LOG.error(">>> Error!!! Loaddata1, exitcode={}", exitVal);
				}
			} else {
				LOG.warn(" >>> loadDataProcess1 is already Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, Loaddata1, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, Loaddata1, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
		}
	}
//	public void loadData2() {
//
//		try {
//			if (loadDataProcess2 == null || !loadDataProcess2.isAlive()) {
//				ProcessBuilder builder = new ProcessBuilder();
//				builder.command("sh", "-c", ignite2LoaddataScript);
//
//				builder.directory(new File(partyContactBaseDir));
//				loadDataProcess2 = builder.start();
//
//				loadDataExecutor2 = Executors.newSingleThreadExecutor();
//				loadDataExecutor2.submit(new Runnable() {
//
//					@Override
//					public void run() {
//						BufferedReader reader = new BufferedReader(new InputStreamReader(loadDataProcess2.getInputStream()));
//						reader.lines().forEach(str -> LOG.info(str));
//					}
//
//				});
//
//				int exitVal = loadDataProcess2.waitFor();
//				if (exitVal == 0) {
//					LOG.info(">>> Success!!! Loaddata2");
//				} else {
//					LOG.error(">>> Error!!! Loaddata2, exitcode={}", exitVal);
//				}
//			} else {
//				LOG.warn(" >>> loadDataProcess2 is already Running.");
//			}
//		} catch (IOException e) {
//			LOG.error(">>> Error!!!, Loaddata2, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//		} catch (InterruptedException e) {
//			LOG.error(">>> Error!!!, Loaddata2, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
//		}
//	}
//
//	public void startIgnite2() {
//
//		if (igniteCtx2 == null || !igniteCtx2.isActive()) {
//			igniteCtx2 =new FileSystemXmlApplicationContext("file:"+igniteConfigFile2);
//			//	new FileSystemXmlApplicationContext("file:/home/steven/gitrepo/transglobe/streamingetl-pcr420669/env-dev1/config/ignite-config-1.xml");
//			//		            new ClassPathXmlApplicationContext("ignite-config-1.xml");
//			//			try {
//			// Get ignite from Spring (note that local cluster node is already started).
//			Ignite ignite2 = (Ignite)igniteCtx2.getBean("igniteBean2");
//
//			LOG.info(">>> ignite2 started!!!");
//
//		} else {
//			LOG.info(">>> igniteCtx2 is still active. Cannot start");
//		}
//
//	}
//	public void stopIgnite2() {
//
//		if (igniteCtx2 == null || !igniteCtx2.isActive()) {
//			LOG.info(">>> igniteCtx2 is null or is not active.");
//
//		} else {
//			igniteCtx2.close();
//
//			LOG.info(">>> igniteCtx2 close.");
//		}
//	}
}
