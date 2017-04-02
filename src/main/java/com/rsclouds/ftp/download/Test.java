package com.rsclouds.ftp.download;

import java.io.IOException;

public class Test {
	public static void main(String[] args) throws IOException {
		DownloadBeanFTP bean = new DownloadBeanFTP("192.168.2.4", 21, "ftp",
				"rsclouds@456", "/test/",
				"VS2008ProEdition90DayTrialCHSX1435983.iso",
				"/Users/chenshang/Downloads",
				"VS2008ProEdition90DayTrialCHSX1435983.iso", 4);
		StartDownloadFTP sd = new StartDownloadFTP(bean,"jobid",
				"downloadURL", "rowkey", "saveFilename", "path");// 创建开始下载对象
		sd.startDownload();// 开始下载
	}
}
