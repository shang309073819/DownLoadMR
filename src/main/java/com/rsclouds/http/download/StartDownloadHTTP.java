﻿/*
 * 开始下载文件
 * 2014-6-27 made by chenshangshang
 */
package com.rsclouds.http.download;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rsclouds.Import;
import com.rsclouds.hbase.api.HbaseBase;
import com.rsclouds.hbase.api.HbaseConfig;

public class StartDownloadHTTP {
	private DownloadBeanHTTP bean;// 封装下载参数信息实体
	private File positionFile;// 记录下载位置文件
	private long[] startPos;// 开始位置数组
	private long[] endPos;// 结束位置数组
	private static int sleepTime = 5000;// 每隔5秒向记录下载位置的缓存文件写入各个下载线程当前的下载位置
	private DownloadThreadHTTP[] downloadThread;// 用于下载文件的线程数组
	private long fileLength = 0;// 总的字节数
	@SuppressWarnings("unused")
	private boolean first = true;// 是否刚开始下载，如果不是刚开始下载则为false(即已经开始下载且暂停过)
	private boolean stop = false;// 下载结束标志
	private String jobid; // hbase的rowkey
	private String downloadURL = null;
	private String rowkey = null;
	private String saveFilename = null;
	private String path = null;

	// =======下载进度信息
	private long beginTime;
	private File saveFile;// 保存文件对象
	public static long downloaded = 0; // 已经下载的字节数(全局)
	public static long downloadednow = 0;// 断点下载的已下载字节数（局部）为了计算当前下载速度

	private final Log log = LogFactory.getLog(getClass().getName());

	// 构造方法
	public StartDownloadHTTP(DownloadBeanHTTP bean, String jobid,
			String downloadURL, String rowkey, String saveFilename, String path) {
		this.bean = bean;
		this.jobid = jobid;
		this.downloadURL = downloadURL;
		this.rowkey = rowkey;
		this.saveFilename = saveFilename;
		this.path = path;
		// 创建saveFile
		saveFile = new File(new File(this.bean.getSavePath()),
				bean.getSaveFilename());
		// 创建saveFile.lck，用于记录下载位置
		positionFile = new File(new File(this.bean.getSavePath()),
				this.bean.getSaveFilename() + ".lck");
	}

	// 开始下载
	@SuppressWarnings("deprecation")
	public void startDownload() throws IOException {
		// 建立连接请求
		HttpURLConnection conn = null;
		try {
			log.info("开始建立HTTP连接");
			URL url = new URL(this.bean.getSourceUrl());// 获取资源路径
			conn = (HttpURLConnection) url.openConnection();
		} catch (Exception e) {
			log.info("主机连接异常");
			log.info("异常为:" + e.getMessage());
			Map<String, String> map = new HashMap<String, String>();
			map.put(HbaseConfig.JOB_META_STATE, "error");
			// 插入记录记录
			HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
					HbaseConfig.JOB_META, map);
			log.info("写入数据库error");
			log.info("退出状态码为1");
			System.exit(1);
		}
		log.info("HTTP连接正常");
		int count = 0;
		log.info("开始获取HTTP服务器文件大小");
		while ((fileLength <= 0) && (count < 10)) {
			// 获取文件长度，单位是byte
			fileLength = conn.getContentLengthLong();
			log.info("正在重试获取文件长度,重试" + count + "次");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			count++;
		}

		log.info("下载文件的大小:" + getFileFormat(fileLength));
		log.info("下载路径为:" + downloadURL);

		if (fileLength <= 0) {
			log.info("服务器不能返回文件大小");
			Map<String, String> map = new HashMap<String, String>();
			map.put(HbaseConfig.JOB_META_STATE, "error");
			// 插入记录记录
			HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
					HbaseConfig.JOB_META, map);
			log.info("写入数据库error");
			log.info("退出状态码为1");
			System.exit(1);
		}

		log.info("分配线程。线程数量=" + bean.getThreadNum() + ",总字节数=" + fileLength
				+ ",字节/线程=" + fileLength / bean.getThreadNum());

		// 如果缓存文件已经存在，表明之前已经下载过一部分
		if (positionFile.exists()) {
			first = false;
			// 读取缓存文件中的下载位置，即每个下载线程的开始位置和结束位置，
			// 将读取到的下载位置写入到开始数组和结束数组
			readDownloadPosition();
		} else {
			// 如果是刚开始下载
			getDownloadPosition();// 获取下载位置
		}

		if (!stop) {
			// 创建下载线程数组,每个下载线程负责各自的文件块下载
			downloadThread = new DownloadThreadHTTP[bean.getThreadNum()];
			for (int i = 0; i < bean.getThreadNum(); i++) {
				downloadThread[i] = new DownloadThreadHTTP(bean.getSourceUrl(),
						bean.getSavePath() + File.separator
								+ bean.getSaveFilename(), startPos[i],
						endPos[i], i);

				downloadThread[i].start();// 启动线程，开始下载
				beginTime = System.currentTimeMillis();
				log.info("线程" + i + "开始下载");
			}
			// 向缓存文件循环写入下载文件位置信息
			while (!stop) {
				try {
					writeDownloadPosition();// 更新下载位置信息
					Thread.sleep(sleepTime);// 每隔10秒更新一次下载位置信息
					// 获取下载信息，输出到控制台
					log.info(getDesc());
					// 返回下载进度
					String urlsString = "http://192.168.2.4:8080/webserver/v1/"
							+ jobid + "?op=PUSH_PROGRESS&path="
							+ URLEncoder.encode(downloadURL) + "&progress="
							+ getProgress(fileLength, downloaded);
					URL url = new URL(urlsString);
					HttpURLConnection httpCon = (HttpURLConnection) url
							.openConnection();
					httpCon.setRequestMethod("PUT");
					int responseCode = httpCon.getResponseCode();
					if (responseCode == 200) {
						// 打印log日志
						log.info("返回路径为:" + urlsString);
						log.info("写入进度成功");
					}
					stop = true;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				for (int i = 0; i < startPos.length; i++)// 判断是否所有下载线程都执行结束
				{
					if (!downloadThread[i].isDownloadOver()) {
						stop = false;// 只要有一个下载线程没有执行结束，则文件还没有下载完毕
						break;
					}
				}
			}
			log.info("所有下载线程执行完毕,文件下载完成");
			// 写入进度100
			log.info("开始返回100进度");
			String urlsString = "http://192.168.2.4:8080/webserver/v1/" + jobid
					+ "?op=PUSH_PROGRESS&path="
					+ URLEncoder.encode(downloadURL) + "&progress=100";
			log.info("返回的路径为:" + urlsString);
			URL url = new URL(urlsString);
			HttpURLConnection httpCon = (HttpURLConnection) url
					.openConnection();
			httpCon.setRequestMethod("PUT");
			int responseCode = httpCon.getResponseCode();
			if (responseCode == 200) {
				// 打印log日志
				log.info("写入进度100成功");
			}

			// 上传到Gt-data
			try {
				log.info("开始上传文件到Gt-data");
				log.info("本地路径为:/home/yarn/temp/" + saveFilename);
				log.info("上传路径为:" + path + saveFilename);

				Import importData = new Import();
				boolean flag = importData.ImportFromLocal("/home/yarn/temp/"
						+ saveFilename, path + saveFilename);
				if (flag) {
					log.info("上传Gt-data成功");
					log.info("写入数据库成功");
					Map<String, String> map = new HashMap<String, String>();
					map.put(HbaseConfig.JOB_META_STATE, "success");
					// 插入记录记录
					HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
							HbaseConfig.JOB_META, map);

					// 删除下载位置缓存文件
					boolean dflag = saveFile.delete();
					boolean dflag1 = positionFile.delete();
					if (dflag && dflag1) {
						log.info("临时文件删除成功");
					} else {
						log.info("临时文件删除失败");
					}
				} else {
					log.info("上传Gt-data失败");
					log.info("写入数据库失败");
					Map<String, String> map = new HashMap<String, String>();
					map.put(HbaseConfig.JOB_META_STATE, "error");
					// 插入记录记录
					HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
							HbaseConfig.JOB_META, map);
					log.info("退出状态码为1");
					System.exit(1);
				}
			} catch (Exception e) {
				log.info("上传Gt-data异常");
				log.info("异常为:" + e.getMessage());
				Map<String, String> map = new HashMap<String, String>();
				map.put(HbaseConfig.JOB_META_STATE, "error");
				// 插入记录记录
				HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
						HbaseConfig.JOB_META, map);
				log.info("写入数据库error");
				log.info("退出状态码为1");
				System.exit(1);
			}
		}
	}

	// 更新下载位置缓存文件的下载位置
	public void writeDownloadPosition() {
		try {
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(
					positionFile));
			// 将一个 int 值以 4-byte 值形式写入基础输出流中，先写入高字节。
			// 如果没有抛出异常，则计数器 written 增加 4。
			dos.writeInt(bean.getThreadNum());
			for (int i = 0; i < bean.getThreadNum(); i++) {
				// 将一个 long 值以 8-byte 值形式写入基础输出流中，先写入高字节。
				// 如果没有抛出异常，则计数器 written增加 8。
				dos.writeLong(downloadThread[i].getStartPos());
				dos.writeLong(downloadThread[i].getEndPos());
			}
			dos.writeLong(downloaded);
			dos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 读取已经存在的缓存文件中的下载位置
	private void readDownloadPosition() throws IOException {
		try {
			DataInputStream dis = new DataInputStream(new FileInputStream(
					positionFile));
			// 获取下载位置的数目，即有多少个开始位置，多少个结束位置
			int DownloadNum = dis.readInt();
			startPos = new long[DownloadNum];
			endPos = new long[DownloadNum];
			for (int i = 0; i < DownloadNum; i++)// 获取开始位置数组和结束位置数组
			{
				startPos[i] = dis.readLong();
				endPos[i] = dis.readLong();
			}
			downloaded = dis.readLong();
			dis.close();
		} catch (Exception e) {
			log.info("读取已经存在的缓存文件中的下载位置");
			log.info("异常为:" + e.getMessage());
			log.info("开始删除缓存文件，请重试下载");
			// 删除下载位置缓存文件
			boolean dflag = saveFile.delete();
			boolean dflag1 = positionFile.delete();
			if (dflag && dflag1) {
				log.info("临时文件删除成功");
			} else {
				log.info("临时文件删除失败");
			}
			Map<String, String> map = new HashMap<String, String>();
			map.put(HbaseConfig.JOB_META_STATE, "error");
			// 插入记录记录
			HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
					HbaseConfig.JOB_META, map);
			log.info("写入数据库error");
			log.info("退出状态码为1");
			System.exit(1);
		}
	}

	// 获取下载位置(刚开始下载的情况)的起始指针和结束指针，并存放到数组中
	private void getDownloadPosition() {
		startPos = new long[bean.getThreadNum()];// 创建开始位置数组
		endPos = new long[bean.getThreadNum()];// 创建结束位置数组
		if (fileLength == -1) {
			stop = true;
		} else if (fileLength == -2) {
			stop = true;
		} else if (fileLength > 0) {
			for (int i = 0, len = bean.getThreadNum(); i < len; i++) {
				long size = i * (fileLength / len);
				startPos[i] = size;
				// 设置最后一个结束点的位置
				if (i == len - 1) {
					endPos[i] = fileLength;
				} else {
					size = (i + 1) * (fileLength / len);
					endPos[i] = size;
				}
			}
		} else {
			stop = true;
		}
	}

	// 获取相关下载信息
	private String getDesc() {
		return String
				.format("已下载/总大小=%s/%s(%s),速度:%s,耗时:%s,剩余大小:%d",
						getFileFormat(downloaded),
						getFileFormat(fileLength),
						(getProgress(fileLength, downloaded) + "%"),
						getFileFormat(downloadednow
								/ ((System.currentTimeMillis() - beginTime) / 1000 + 1)),
						getTime((System.currentTimeMillis() - beginTime) / 1000),
						fileLength - downloaded);
	}

	// 格式化输出
	private String getFileFormat(long totals) {
		// 计算文件大小
		int i = 0;
		String j = "BKMGT";
		float s = totals;
		while (s > 1024) {
			s /= 1024;
			i++;
		}
		return String.format("%.2f", s) + j.charAt(i);
	}

	// 下载进度
	private String getProgress(long totals, long read) {
		if (totals == 0)
			return "0";
		return String.format("%d", read * 100 / totals);
	}

	// 耗时
	private String getTime(long seconds) {
		int i = 0;
		String j = "秒分时天";
		long s = seconds;
		String result = "";
		while (s > 0) {
			if (s % 60 > 0) {
				result = String.valueOf(s % 60) + (char) j.charAt(i) + result;
			}
			s /= 60;
			i++;
		}
		return result;
	}

}
