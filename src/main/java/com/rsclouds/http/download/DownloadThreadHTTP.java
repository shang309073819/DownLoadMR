/*
 * 下载线程
 * 2014-6-27 made by chenshangshang
 */

package com.rsclouds.http.download;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;

public class DownloadThreadHTTP extends Thread {
	// HTTP的信息
	private String sourceUrl;// 资源路径
	private long startPos;// 下载开始位置
	private long endPos;// 下载结束位置
	private int threadId;// 线程ID
	private static int BUFF_LENGTH = 1024 * 8;// 缓冲区大小 8k
	private boolean downloadOver = false;// 该线程是否下载完毕
	private RandomAccessFile saveFile;// 保存文件对象

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

	// HTTP构造函数
	public DownloadThreadHTTP(String sourceUrl, String fullName, long startPos,
			long endPos, int threadId) throws IOException {
		this.sourceUrl = sourceUrl;
		this.startPos = startPos;
		this.endPos = endPos;
		this.threadId = threadId;

		saveFile = new RandomAccessFile(fullName, "rw");
		// 定位写入文件的位置
		saveFile.seek(startPos);
	}

	// 启动线程
	public void run() {
		try {
			URL url = new URL(this.sourceUrl);// 创建URL对象

			// 默认的会处理302请求
			HttpURLConnection.setFollowRedirects(false);
			HttpURLConnection httpConnection = null;
			httpConnection = (HttpURLConnection) url.openConnection();
			httpConnection.setRequestProperty("RANGE", "bytes=" + startPos
					+ "-");
			int responseCode = httpConnection.getResponseCode();

			if (responseCode < 200 || responseCode >= 400) {
				throw new IOException("服务器返回无效信息:" + responseCode);
			}
			InputStream inputStream = httpConnection.getInputStream(); // 获取文件输入流，读取文件内容

			byte[] buff = new byte[BUFF_LENGTH];// 创建缓冲区
			int length = -1;
			boolean flag = true;
			while ((length = inputStream.read(buff)) > 0 && startPos < endPos
					&& !downloadOver && flag) {
				if ((startPos + length) >= endPos) {
					saveFile.write(buff, 0, (int) (endPos - startPos));
					flag = false;
				} else {
					saveFile.write(buff, 0, length); // 写入文件内容
				}
				startPos = startPos + length;

				StartDownloadHTTP.downloaded = StartDownloadHTTP.downloaded + length;
				StartDownloadHTTP.downloadednow = StartDownloadHTTP.downloadednow
						+ length;
			}
			System.out.println("线程" + threadId + "执行结束");
			this.downloadOver = true;
			httpConnection.disconnect();
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
}
