package com.transglobe.streamingetl.partycontact.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.stereotype.Service;


@Service
public class PartyContactService {
	static final Logger LOG = LoggerFactory.getLogger(PartyContactService.class);

	@Value("${ignite.config.file1}")
	private String igniteConfigFile1;

	@Value("${ignite.config.file2}")
	private String igniteConfigFile2;

	@Value("${partycontact.base.dir}")
	private String partyContactBaseDir;

	@Value("${ignite1.loaddata.script}")
	private String ignite1LoaddataScript;

	@Value("${ignite2.loaddata.script}")
	private String ignite2LoaddataScript;

	private FileSystemXmlApplicationContext igniteCtx1;

	private FileSystemXmlApplicationContext igniteCtx2;

	private Process loadDataProcess1;
	private ExecutorService loadDataExecutor1;

	private Process loadDataProcess2;
	private ExecutorService loadDataExecutor2;

	public void startIgnite1() {

		if (igniteCtx1 == null || !igniteCtx1.isActive()) {
			igniteCtx1 =new FileSystemXmlApplicationContext("file:"+igniteConfigFile1);

			//			try {
			// Get ignite from Spring (note that local cluster node is already started).
			Ignite ignite1 = (Ignite)igniteCtx1.getBean("igniteBean1");

			LOG.info(">>> ignite1 started!!!");
		} else {
			LOG.info(">>> igniteCtx1 is still active. Cannot start");
		}

	}

	public void stopIgnite1() {

		if (igniteCtx1 == null || !igniteCtx1.isActive()) {
			LOG.info(">>> igniteCtx1 is null or is not active. Cannot stop");

		} else {
			igniteCtx1.close();

			LOG.info(">>> igniteCtx1 close.");
		}
	}
	public void stopLoadData1() {
		if (loadDataProcess1.isAlive()) {
			loadDataProcess1.destroy();
			loadDataExecutor1.shutdown();
			if (!loadDataExecutor1.isTerminated()) {
				loadDataExecutor1.shutdownNow();

				try {
					loadDataExecutor1.awaitTermination(600, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
			LOG.info(">>> LoadData1 stopped");
		} else {
			LOG.info(">>> LoadData1 is NOT alive, cannot stop");
		}
	}
	public void stopLoadData2() {
		if (loadDataProcess2.isAlive()) {
			loadDataProcess2.destroy();
			loadDataExecutor2.shutdown();
			if (!loadDataExecutor2.isTerminated()) {
				loadDataExecutor2.shutdownNow();

				try {
					loadDataExecutor2.awaitTermination(600, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					LOG.error(">>> ERROR!!!, msg={}, stacetrace={}",
							ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
				}

			}
			LOG.info(">>> LoadData2 stopped");
		} else {
			LOG.info(">>> LoadData2 is NOT alive, cannot stop");
		}
	}
	public void loadData1() {

		try {
			if (loadDataProcess1 == null || !loadDataProcess1.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				builder.command("sh", "-c", ignite1LoaddataScript);

				builder.directory(new File(partyContactBaseDir));
				loadDataProcess1 = builder.start();

				loadDataExecutor1 = Executors.newSingleThreadExecutor();
				loadDataExecutor1.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(loadDataProcess1.getInputStream()));
						reader.lines().forEach(str -> LOG.info(str));
					}

				});

				int exitVal = loadDataProcess1.waitFor();
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
	public void loadData2() {

		try {
			if (loadDataProcess2 == null || !loadDataProcess2.isAlive()) {
				ProcessBuilder builder = new ProcessBuilder();
				builder.command("sh", "-c", ignite2LoaddataScript);

				builder.directory(new File(partyContactBaseDir));
				loadDataProcess2 = builder.start();

				loadDataExecutor2 = Executors.newSingleThreadExecutor();
				loadDataExecutor2.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(loadDataProcess2.getInputStream()));
						reader.lines().forEach(str -> LOG.info(str));
					}

				});

				int exitVal = loadDataProcess2.waitFor();
				if (exitVal == 0) {
					LOG.info(">>> Success!!! Loaddata2");
				} else {
					LOG.error(">>> Error!!! Loaddata2, exitcode={}", exitVal);
				}
			} else {
				LOG.warn(" >>> loadDataProcess2 is already Running.");
			}
		} catch (IOException e) {
			LOG.error(">>> Error!!!, Loaddata2, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
		} catch (InterruptedException e) {
			LOG.error(">>> Error!!!, Loaddata2, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
		}
	}

	public void startIgnite2() {

		if (igniteCtx2 == null || !igniteCtx2.isActive()) {
			igniteCtx2 =new FileSystemXmlApplicationContext("file:"+igniteConfigFile2);
			//	new FileSystemXmlApplicationContext("file:/home/steven/gitrepo/transglobe/streamingetl-pcr420669/env-dev1/config/ignite-config-1.xml");
			//		            new ClassPathXmlApplicationContext("ignite-config-1.xml");
			//			try {
			// Get ignite from Spring (note that local cluster node is already started).
			Ignite ignite2 = (Ignite)igniteCtx2.getBean("igniteBean2");

			LOG.info(">>> ignite2 started!!!");

		} else {
			LOG.info(">>> igniteCtx2 is still active. Cannot start");
		}

	}
	public void stopIgnite2() {

		if (igniteCtx2 == null || !igniteCtx2.isActive()) {
			LOG.info(">>> igniteCtx2 is null or is not active.");

		} else {
			igniteCtx2.close();

			LOG.info(">>> igniteCtx2 close.");
		}
	}
}
