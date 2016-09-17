DOWNLOAD_DIRECTORY=downloads
CONFIGURATION_DIRECTORY=configuration
HADOOP_DIRECTORY=hadoop-2.6.0
JAVA_DIRECTORY=/usr/lib/jvm/java-8-openjdk

.PHONY: run
run: start SlidingWindowAggregation/out/artifacts/SlidingWindowAggregation_jar/SlidingWindowAggregation.jar
	$(MAKE) clean-hdfs
	$(MAKE) put-input
	hadoop-2.6.0/bin/yarn jar SlidingWindowAggregation/out/artifacts/SlidingWindowAggregation_jar/SlidingWindowAggregation.jar
	$(MAKE) show

.PHONY: put-input
put-input: start
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -put input.txt /input

.PHONY: clean-hdfs
clean-hdfs: start
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_0
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_1
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_2
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_3
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_4
	-${HADOOP_DIRECTORY}/bin/hdfs dfs -rmr /phase_5

.PHONY: show
show: start
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_0
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_0/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_1
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_1/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_2
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_2/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_3
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_3/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_4
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_4/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -ls /phase_5
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_5/part-r-00000
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_5/part-r-00001
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_5/part-r-00002
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_5/part-r-00003
	hadoop-2.6.0/bin/hdfs dfs -cat /phase_5/part-r-00004

.PHONY: start
start: start-dfs start-yarn

.PHONY: stop
stop: stop-yarn stop-dfs

.PHONY: start-yarn
start-yarn: ${HADOOP_DIRECTORY}/.hadoop
	${HADOOP_DIRECTORY}/sbin/start-yarn.sh

.PHONY: stop-yarn
stop-yarn: ${HADOOP_DIRECTORY}/.hadoop
	${HADOOP_DIRECTORY}/sbin/stop-yarn.sh

.PHONY: start-dfs
start-dfs: ${HADOOP_DIRECTORY}/.hadoop
	${HADOOP_DIRECTORY}/sbin/start-dfs.sh

.PHONY: stop-dfs
stop-dfs: ${HADOOP_DIRECTORY}/.hadoop
	${HADOOP_DIRECTORY}/sbin/stop-dfs.sh

.PHONY: format
format: ${HADOOP_DIRECTORY}/.hadoop
	${HADOOP_DIRECTORY}/bin/hdfs namenode -format

${HADOOP_DIRECTORY}/.hadoop: ${DOWNLOAD_DIRECTORY}/hadoop-2.6.0.tar.gz
	mkdir ${HADOOP_DIRECTORY}
	tar -xvf ${DOWNLOAD_DIRECTORY}/hadoop-2.6.0.tar.gz --strip-components=1 -C ${HADOOP_DIRECTORY}
	sed -i -e "s|^export JAVA_HOME=\$${JAVA_HOME}|export JAVA_HOME=${JAVA_DIRECTORY}|g" ${HADOOP_DIRECTORY}/etc/hadoop/hadoop-env.sh
	cp -f ${CONFIGURATION_DIRECTORY}/core-site.xml ${HADOOP_DIRECTORY}/etc/hadoop/core-site.xml
	cp -f ${CONFIGURATION_DIRECTORY}/hdfs-site.xml ${HADOOP_DIRECTORY}/etc/hadoop/hdfs-site.xml
	cp -f ${CONFIGURATION_DIRECTORY}/mapred-site.xml ${HADOOP_DIRECTORY}/etc/hadoop/mapred-site.xml
	cp -f ${CONFIGURATION_DIRECTORY}/yarn-site.xml ${HADOOP_DIRECTORY}/etc/hadoop/yarn-site.xml
	touch ${HADOOP_DIRECTORY}/.hadoop
	$(MAKE) format

${DOWNLOAD_DIRECTORY}/hadoop-2.6.0.tar.gz:
	wget -P ${DOWNLOAD_DIRECTORY} ftp://ftp.task.gda.pl/pub/www/apache/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz

.PHONY: clean
clean:
	$(MAKE) stop
	rm -rf hadoop-2.6.0