package com;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

//주의 우리는 Hadoop을 하고 있다는 것을 명심하라 import는 거의 hadoop.머시기

public class ConnectionHadoop {
	public static void main(String[] args) {
		PrivilegedExceptionAction<Void> pea = new PrivilegedExceptionAction<Void>() {

			@Override
			public Void run() throws Exception {
				Configuration config = new Configuration();
				config.set("fs.defaultFS", "hdfs://192.168.0.142:9000/user/bdi"); // 기본 경로 설정
//				config.set("hadoop.job.ugi","bdi");
				config.setBoolean("dfs.support.append", true);

				FileSystem fs = FileSystem.get(config);
				Path upFileName = new Path("word.txt");
				Path logFileName = new Path("word.log");
				if (fs.exists(upFileName)) {
					fs.delete(upFileName, true);
					fs.delete(logFileName, true);
				} //해당 파일이 있을 경우 지우고 다시 생성한다는 뜻
				FSDataOutputStream fsdo = fs.create(upFileName);
				fsdo.writeUTF("hi hi hi hey hey lol start hi");
				fsdo.close();

				Path dirName = new Path("/user/bdi");
				FileStatus[] files = fs.listStatus(dirName);
				for (FileStatus file : files) {
					System.out.println(file);
				} //여러 데이터를 한꺼번에 출력해 줌
				return null;
			}
		};    // 무명클래스 기본생성자가 없어 바로 new가 안될 때의 대처

		UserGroupInformation ugi = UserGroupInformation.createRemoteUser("bdi");
		try {
			ugi.doAs(pea);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
