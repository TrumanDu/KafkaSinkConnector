package com.aibibang.bigdata;

/**
 * @author Truman.P.Du
 * @date May 28, 2019 2:41:05 PM
 * @version 1.0
 */
public class Version {
	private static String version = Version.class.getPackage().getImplementationVersion();

	public static String version() {
		return version;
	}
}
