package com.transglobe.streamingetl.partycontact.rest.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtils {
	public static String restService(String urlStr, String requestMethod) throws Exception {

		HttpURLConnection httpConn = null;
		URL url = null;
		try {
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod(requestMethod);
			int responseCode = httpConn.getResponseCode();
			//			LOG.info(">>>>>  responseCode={}",responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			return response.toString();
		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
}
