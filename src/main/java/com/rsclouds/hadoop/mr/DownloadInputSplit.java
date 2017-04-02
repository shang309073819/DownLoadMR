/*
 * 定义Split
 * 2014-7-22 made by chenshangshang
 */
package com.rsclouds.hadoop.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DownloadInputSplit extends FileSplit {

	private String DownloadURl = "http";

	public DownloadInputSplit() {
		super();
	}

	public DownloadInputSplit(String url) {
		super(new Path("hdfs://192.168.2.3:8020/DownLoad"), 0, 10,
				(String[]) null);
		DownloadURl = url;
	}

	public DownloadInputSplit(String url, String[] ip) {
		super(new Path("hdfs://192.168.2.3:8020/DownLoad"), 0, 10, ip);
		DownloadURl = url;
	}

	public String getDownloadURl() {
		return DownloadURl;
	}

	public void setDownloadURl(String downloadURl) {
		DownloadURl = downloadURl;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeUTF(DownloadURl);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		DownloadURl = in.readUTF();
	}
}
