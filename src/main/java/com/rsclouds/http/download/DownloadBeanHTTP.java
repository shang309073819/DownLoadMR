/*
 * 封装下载的相关参数信息
 * 2014-6-27 made by chenshangshang
 */
package com.rsclouds.http.download;

public class DownloadBeanHTTP {

	// HTTP的相关信息
	private String sourceUrl;// 资源路径
	private String saveFilename;// 保存文件名
	private String savePath;// 保存路径
	private int threadNum;// 线程数

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}

	public String getSaveFilename() {
		return saveFilename;
	}

	public void setSaveFilename(String saveFilename) {
		this.saveFilename = saveFilename;
	}

	public String getSavePath() {
		return savePath;
	}

	public void setSavePath(String savePath) {
		this.savePath = savePath;
	}

	public int getThreadNum() {
		return threadNum;
	}

	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}
}
