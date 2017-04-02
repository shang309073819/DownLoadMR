/*
 * 封装下载的相关参数信息
 * 2014-6-27 made by chenshangshang
 */

package com.rsclouds.ftp.download;

public class DownloadBeanFTP {
	private String ip; // ftp地址
	private int port; // ftp端口号
	private String username; // 用户名
	private String password; // 密码
	private String remotePath; // ftp文件存放路径
	private String remoteFileName; // ftp文件名字
	private String localPath; // 本地保存路径
	private String localFileName; // 本地保存文件名称
	private int nSplitter; // 文件分成几段，默认是4段

	public DownloadBeanFTP() {
		// default 5
		this("", 21, "", "", "", "", "", "", 4);
	}

	public DownloadBeanFTP(String ip, int port, String username,
			String password, String remotePath, String remoteFileName,
			String localPath, String localFileName, int nSplitter) {
		this.ip = ip;
		this.port = port;
		this.username = username;
		this.password = password;
		this.remotePath = remotePath;
		this.remoteFileName = remoteFileName;
		this.localPath = localPath;
		this.localFileName = localFileName;
		this.nSplitter = nSplitter;
	}

	public String getRemotePath() {
		return remotePath;
	}

	public void setRemotePath(String remotePath) {
		this.remotePath = remotePath;
	}

	public String getRemoteFileName() {
		return remoteFileName;
	}

	public void setRemoteFileName(String remoteFileName) {
		this.remoteFileName = remoteFileName;
	}

	public String getLocalFileName() {
		return localFileName;
	}

	public void setLocalFileName(String localFileName) {
		this.localFileName = localFileName;
	}

	public int getnSplitter() {
		return nSplitter;
	}

	public void setnSplitter(int nSplitter) {
		this.nSplitter = nSplitter;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

}