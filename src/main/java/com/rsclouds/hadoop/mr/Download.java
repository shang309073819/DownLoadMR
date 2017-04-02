/*
 * MR的主程序入口
 * 2014-7-22 made by chenshangshang
 */
package com.rsclouds.hadoop.mr;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.rsclouds.ftp.download.DownloadBeanFTP;
import com.rsclouds.ftp.download.StartDownloadFTP;
import com.rsclouds.hbase.api.HbaseBase;
import com.rsclouds.hbase.api.HbaseConfig;
import com.rsclouds.hbase.api.HbaseUtils;
import com.rsclouds.http.download.DownloadBeanHTTP;
import com.rsclouds.http.download.StartDownloadHTTP;

import org.apache.commons.logging.Log;

public class Download extends Configured implements Tool {
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		Configuration conf = (getConf() == null ? new Configuration()
				: getConf());
		conf.set("mapreduce.map.maxattempts", "0");
		conf.set("mapreduce.map.failures.maxpercent", "100");
		conf.setInt("mapred.task.timeout", Integer.MAX_VALUE);

		// 记录参数的长度
		int length = args.length;
		conf.setInt("length", length);

		if (length > 2) {
			conf.set("jobid", args[0]);
			conf.set("path", args[1]);
			for (int i = 2; i < args.length; i++) {
				String url = "url" + i;
				conf.set(url, URLDecoder.decode(args[i]));
			}
		} else if (length > 0) {
			String jobid = args[0];
			String url = URLDecoder.decode(args[1]);
			conf.set("url", url);
			conf.set("jobid", jobid);
			String rowkey = jobid + "_" + url;

			String ip = null;
			String path = null;

			// 搜索rowkey包含指定关键字的记录
			System.out.println("搜索rowkey包含指定关键字的记录:");
			for (Result rs : HbaseBase.selectByRowFilter(HbaseConfig.JOB_TABLE,
					rowkey)) {
				ip = HbaseUtils.result2Job(rs).getNode();
				path = HbaseUtils.result2Job(rs).getOutPath();
			}
			if (path != null) {
				int index_3 = path.lastIndexOf("/");
				path = path.substring(0, index_3 + 1);
			}
			conf.set("ip", ip);
			conf.set("path", path);

		} else {
			System.out
					.println("PLEASE USE hadoop jar DownLoadMR.jar <jobid> <outpath> <url1> <url2> ...");
			System.exit(3);
		}

		Job job = Job.getInstance(conf, "DownLoad_MR");
		job.setJarByClass(Download.class);
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path("/DownLoad/output/" + args[0]), true);
		FileInputFormat.addInputPath(job, new Path("/DownLoad"));
		FileOutputFormat.setOutputPath(job, new Path("/DownLoad/output/"
				+ args[0]));

		// 设置输入的InputFormat
		job.setInputFormatClass(DownloadInputFormat.class);
		
		job.setMapperClass(DownloadMR.DownloadMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Download(), args);
	}
}

// 下载的MR
class DownloadMR {

	static class DownloadMapper extends
			Mapper<NullWritable, Text, NullWritable, Text> {
		String jobid = null;
		String rowkey = null;
		String downloadURL = null;
		String saveFilename = null;
		String path = null;
		Boolean MAP_SWITCH = false;

		private final Log log = LogFactory.getLog(getClass().getName());

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}

		@SuppressWarnings("deprecation")
		@Override
		protected void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			// 当前运行MR程序的JID
			JobID JID = context.getJobID();
			// 作为rowkey的jobid
			jobid = conf.get("jobid");
			// 下载路径
			downloadURL = value.toString();
			// Gt-data 上传路径
			path = conf.get("path");

			// 获得本机IP
			InetAddress addr = InetAddress.getLocalHost();
			String ip = addr.getHostAddress().toString();

			// 获取当前运行的PID
			String name = ManagementFactory.getRuntimeMXBean().getName();
			String pid = name.split("@")[0];
			log.info("当前运行的进程为:" + pid);

			// Hbase操作
			rowkey = jobid + "_" + downloadURL;
			Map<String, String> map = new HashMap<String, String>();
			map.put(HbaseConfig.JOB_META_PID, pid);
			map.put(HbaseConfig.JOB_META_TYPE, "DOWNLOAD");
			map.put(HbaseConfig.JOB_META_NODE, ip);
			map.put(HbaseConfig.JOB_META_STATE, "start");
			map.put(HbaseConfig.JOB_META_JID, JID.toString());
			// 插入记录记录
			HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
					HbaseConfig.JOB_META, map);

			String sourceUrl = downloadURL;
			String urlString = sourceUrl.substring(0, 3);
			if (urlString.equalsIgnoreCase("htt")) {
				saveFilename = getFileNameFromUrl(sourceUrl);// 保存的文件名
				String savePath = "/home/yarn/temp";// 保存的路径
				int threadNum = 4;// 开启的线程数

				// 封装下载相关参数信息
				DownloadBeanHTTP bean = new DownloadBeanHTTP();
				bean.setSourceUrl(sourceUrl);
				bean.setSaveFilename(saveFilename);
				bean.setSavePath(savePath);
				bean.setThreadNum(threadNum);

				// 读取之前的IP
				String ip_old = conf.get("ip");
				// 断点续传，执行拷贝工作
				if (ip_old != null && !ip_old.equalsIgnoreCase(ip)) {
					String commands = "scp " + ip_old + ":/home/yarn/temp/"
							+ saveFilename + " " + ip_old + ":/home/yarn/temp/"
							+ saveFilename + ".lck yarn@" + ip
							+ ":/home/yarn/temp";
					log.info("scp复制命令为:" + commands);
					long startTime = System.currentTimeMillis();
					log.info("开始远程复制");
					Process process = Runtime.getRuntime().exec(commands);
					int code = 0;
					try {
						code = process.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (code == 0) {
						long endTime = System.currentTimeMillis();
						log.info("远程复制成功");
						log.info("复制耗时:" + (endTime - startTime));
					}
				}

				Map<String, String> map2 = new HashMap<String, String>();
				map2.put(HbaseConfig.JOB_META_OUT_PATH, path + saveFilename);
				HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
						HbaseConfig.JOB_META, map2);

				log.info("启动HTTP下载程序");

				// 使用HTTP下载
				StartDownloadHTTP sd = new StartDownloadHTTP(bean, jobid,
						downloadURL, rowkey, saveFilename, path);// 创建开始下载对象
				sd.startDownload();// 开始下载

			} else if (urlString.equalsIgnoreCase("ftp")) {
				if (sourceUrl.contains("%")) {
					sourceUrl = URLDecoder.decode(sourceUrl);
				}

				// 使用FTP下载
				int index_1 = sourceUrl.lastIndexOf("?");
				String url_1 = sourceUrl.substring(0, index_1);
				int index_1_1 = url_1.indexOf("/", 6);
				int index_1_2 = url_1.lastIndexOf("/");

				// 把域名解析成ip地址
				String ipString1 = url_1.substring(6, index_1_1);
				String ipString = null;
				int number = 0;
				while ((ipString == null) & (number <= 5)) {
					try {
						InetAddress localAddress = InetAddress
								.getByName(ipString1);
						ipString = localAddress.getHostAddress();
					} catch (Exception e) {
						log.info("域名解析异常");
						log.info("异常为:" + e.getMessage());
						log.info("重试第" + number + "次");
					}
					number++;
				}

				if (ipString == null) {
					Map<String, String> map3 = new HashMap<String, String>();
					map3.put(HbaseConfig.JOB_META_STATE, "error");
					// 插入记录记录
					HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
							HbaseConfig.JOB_META, map3);
					log.info("写入数据库error");
					log.info("退出状态码为1");
					System.exit(1);
				}

				saveFilename = getFileNameFromUrl(url_1);
				String remotePath = url_1.substring(index_1_1, index_1_2 + 1);

				String url_2 = sourceUrl.substring(index_1 + 1);
				int index_2 = url_2.indexOf("&");

				String url_3 = url_2.substring(0, index_2);
				String url_4 = url_2.substring(index_2 + 1);

				int index_3 = url_3.lastIndexOf("=");
				String username = url_3.substring(index_3 + 1);

				int index_4 = url_4.lastIndexOf("=");
				String password = url_4.substring(index_4 + 1);

				// 读取之前的IP
				String ip_old = conf.get("ip");
				// 断点续传，执行拷贝工作
				if (ip_old != null && !ip_old.equalsIgnoreCase(ip)) {
					String commands = "scp " + ip_old + ":/home/yarn/temp/"
							+ saveFilename + " " + ip_old + ":/home/yarn/temp/"
							+ saveFilename + ".lck yarn@" + ip
							+ ":/home/yarn/temp";
					log.info("scp复制命令为:" + commands);
					log.info("开始远程复制");
					Process process = Runtime.getRuntime().exec(commands);
					int code = 0;
					try {
						code = process.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if (code == 0) {
						log.info("远程复制成功");
					}
				}

				Map<String, String> map2 = new HashMap<String, String>();
				map2.put(HbaseConfig.JOB_META_OUT_PATH, path + saveFilename);
				HbaseBase.writeRows(HbaseConfig.JOB_TABLE, rowkey,
						HbaseConfig.JOB_META, map2);

				log.info("启动FTP下载程序");
				DownloadBeanFTP bean = new DownloadBeanFTP(ipString, 21,
						username, password, remotePath, saveFilename,
						"/home/yarn/temp", saveFilename, 4);
				StartDownloadFTP sd = new StartDownloadFTP(bean, jobid,
						downloadURL, rowkey, saveFilename, path);// 创建开始下载对象
				sd.startDownload();// 开始下载
			} else {
				System.out.println("Please Use HTTP of FTP download");
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
		}

		public static String getFileNameFromUrl(String url) {
			String name = new Long(System.currentTimeMillis()).toString()
					+ ".X";
			int index_1 = url.lastIndexOf("/");

			if (index_1 > 0) {
				name = url.substring(index_1 + 1);
				int index_2 = name.lastIndexOf("?");
				if (index_2 != -1) {
					name = name.substring(0, index_2);
				}
				if (name.trim().length() > 0) {
					return name;
				}
			}
			return name;
		}
	}
}
