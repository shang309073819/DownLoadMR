/*
 * 分片、定义输出格式
 * 2014-7-22 made by chenshangshang
 */
package com.rsclouds.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class DownloadInputFormat extends FileInputFormat<NullWritable, Text> {

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();

		Configuration conf = job.getConfiguration();
		int length = conf.getInt("length", 0);
		if (length > 2) {
			for (int i = 2; i < length; i++) {
				String url = "url" + i;
				String urlString = conf.get(url);
				DownloadInputSplit downloadInputSplit = new DownloadInputSplit(
						urlString);
				splits.add(downloadInputSplit);
			}// 断点续传
		} else if (length > 0) {
			String urlString = conf.get("url");
			DownloadInputSplit downloadInputSplit = new DownloadInputSplit(
					urlString);
			splits.add(downloadInputSplit);
		}
		return splits;
	}

	@Override
	public RecordReader<NullWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new MyRecordReader();
	}

	// 进行是否进行分片
	protected boolean isSplitable(JobContext context, Path filename) {
		return true;
	}

}

class MyRecordReader extends RecordReader<NullWritable, Text> {
	private DownloadInputSplit split; // 定义输入分片
	@SuppressWarnings("unused")
	private Configuration conf; // 定义环境变量
	private Text value; // 定义值
	private boolean flag; // 创建一个辅助布尔值

	@Override
	// 初始化方法执行一次
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (DownloadInputSplit) split; // 强制类型转换
		this.conf = context.getConfiguration(); // 获取分片环境
	}

	@Override
	// 获取键值对的方法
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!flag) {// 判断是否处理完毕
			String str = new String(split.getDownloadURl());// 创建辅助字符串
			value = new Text(str);// 对值进行赋值
			flag = true; // 更改表计量
			return true; // 返回值
		}
		return false; // 返回值
	}

	@Override
	// 默认的返回键的方法
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get(); // 返回空值
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value; // 返回当前值
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// 对过程进行监视
		return flag ? 0 : 1;
	}

	@Override
	public void close() throws IOException {
		// 默认的关闭方法 空实现
	}
}
