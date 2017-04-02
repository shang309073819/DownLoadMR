/*
 * 下载线程
 * 2014-6-27 made by chenshangshang
 */

package com.rsclouds.ftp.download;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class DownloadThreadFTP extends Thread {
	String ip; // 下载文件的地址
	int port; // 地址端口号
	String username; // 用户名
	String password; // 密码
	String remoteFile; // 远程文件路径+文件名称
	private long startPos;// 下载开始位置
	private long endPos;// 下载结束位置
	private int threadId;// 线程ID
	private static int BUFF_LENGTH = 1024 * 8;// 缓冲区大小 8k
	private boolean downloadOver = false;// 该线程是否下载完毕
	private RandomAccessFile saveFile;// 保存文件对象
	public FTPClient ftpClient = new FTPClient();
	private Log log = LogFactory.getLog(getClass().getName());

	public DownloadThreadFTP(String ip, int port, String username,
			String password, String remoteFile, String fullName, long startPos,
			long endPos, int threadId, Object object) throws IOException {

		this.ip = ip;
		this.port = port;
		this.username = username;
		this.password = password;
		this.remoteFile = remoteFile;
		this.startPos = startPos;
		this.endPos = endPos;
		this.threadId = threadId;

		saveFile = new RandomAccessFile(fullName, "rw");
		saveFile.seek(startPos);

	}

	public boolean connect(String hostname, int port, String username,
			String password) throws IOException {
		ftpClient.connect(hostname, port);
		ftpClient.setControlEncoding(System.getProperty("file.encoding"));
		if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
			if (ftpClient.login(username, password)) {
				return true;
			}
		}
		disconnect();
		return false;
	}

	public void disconnect() throws IOException {
		if (ftpClient.isConnected()) {
			ftpClient.disconnect();
		}
	}

	public void run() {
		try {
			log.info("Thread " + threadId + " url start >> " + startPos
					+ "------end >> " + endPos);
			connect(ip, port, username, password);
			// 设置被动模式
			ftpClient.enterLocalPassiveMode();
			// 设置以二进制方式传输
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpClient.rest(String.valueOf(startPos));// 设置开始点偏移量

			InputStream is = ftpClient.retrieveFileStream(new String(remoteFile
					.getBytes("UTF-8"), "iso-8859-1"));

			byte[] buff = new byte[BUFF_LENGTH];// 创建缓冲区
			int length = -1;
			boolean flag = true;
			while ((length = is.read(buff)) > 0 && startPos < endPos
					&& !downloadOver && flag) {
				if ((startPos + length) >= endPos) {
					saveFile.write(buff, 0, (int) (endPos - startPos));
					flag = false;
				} else {
					saveFile.write(buff, 0, length); // 写入文件内容
				}
				startPos = startPos + length;
				// 写入已经下载的文件。
				StartDownloadFTP.downloaded = StartDownloadFTP.downloaded
						+ length;
				StartDownloadFTP.downloadednow = StartDownloadFTP.downloadednow
						+ length;
			}
			// System.out.println("线程" + threadId + "执行结束");
			this.downloadOver = true;
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				// 关闭打开的文件
				saveFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// 返回该线程下载是否完成的标志
	public boolean isDownloadOver() {
		return downloadOver;
	}

	// 获取当前开始位置
	public long getStartPos() {
		return startPos;
	}

	// 获取结束位置
	public long getEndPos() {
		return endPos;
	}
}
