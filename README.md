# MapReduce
初学MR的一些实践
# HDFS

## 1 HDFS概述

### 1.1 HDFS产生背景

​        随着数据量越来越大，在一个操作系统存不下所有的数据，那么就分配到更多的操作系统管理的磁盘中，但是不方便管理和维护，迫切需要一种系统来管理多台机器上的文件，这就是分布式文件管理系统。<u>HDFS只是分布式文件管理系统中的一种。</u>

### 1.2 HDFS定义

HDFS（Hadoop Distributed File System），它是一个文件系统，用于存储文件，<u>通过目录树来定位文件</u>；其次，它是分布式的，由很多服务器联合起来实现其功能，集群中的服务器有各自的角色。

### 1.3 HDFS的使用场景

适合一次写入，多次读出的场景，且不支持文件的修改。适合用来做数据分析，并不适合用来做网盘应用。

## 2 HDFS优缺点

### 2.1 优点

2.1.1 高容错

（1）数据自动保存多个副本。它通过增加副本的形式，提高容错性。

（2）某一个副本丢失以后，它可以自动恢复。

2.1.2 适合处理大数据

（1）数据规模：能够处理数据规模达到GB、TB、甚至PB级别的数据；

（2）文件规模：能够处理百万规模以上的文件数量，数量相当之大。

2.1.3 可构建在廉价机器上，通过多副本机制，提高可靠性。

### 2.2 缺点

2.2.1 不适合低延时数据访问，比如毫秒级的存储数据，是做不到的。

2.2.2 无法高效的对大量小文件进行存储。

（1）存储大量小文件的话，它会占用NameNode大量的内存来存储文件目录和块信息。这样是不可取的，因为NameNode的内存总是有限的；

（2）小文件存储的寻址时间会超过读取时间，它违反了HDFS的设计目标。

2.2.3 不支持并发写入、文件随机修改

（1）一个文件只能有一个写，不允许多个线程同时写；

（2）仅支持数据append（追加），不支持文件的随机修改。

## 3 HDFS组成架构

![1569415294344](D:\BigData\image\1.png)

![1569415424528](D:\BigData\image\1569415424528.png)

4 HDFS块大小的设置

![1569415480432](D:\BigData\image\1569415480432.png)

## 4 HDFS中的block、packet、chunk

很多博文介绍HDFS读写流程上来就直接从文件分块开始，其实，要把读写过程细节搞明白前，你必须知道block、packet与chunk。下面分别讲述。

```
block
这个大家应该知道，文件上传前需要分块，这个块就是block，一般为128MB，当然你可以去改，不过不推荐。因为块太小：寻址时间占比过高。块太大：Map任务数太少，作业执行速度变慢。它是最大的一个单位。

packet
packet是第二大的单位，它是client端向DataNode，或DataNode的PipLine之间传数据的基本单位，默认64KB。

chunk
chunk是最小的单位，它是client向DataNode，或DataNode的PipLine之间进行数据校验的基本单位，默认512Byte，因为用作校验，故每个chunk需要带有4Byte的校验位。所以实际每个chunk写入packet的大小为516Byte。由此可见真实数据与校验值数据的比值约为128 : 1。（即64*1024 / 512）
```

例如，在client端向DataNode传数据的时候，HDFSOutputStream会有一个chunk buff，写满一个chunk后，会计算校验和并写入当前的chunk。之后再把带有校验和的chunk写入packet，当一个packet写满后，packet会进入dataQueue队列，其他的DataNode就是从这个dataQueue获取client端上传的数据并存储的。同时一个DataNode成功存储一个packet后之后会返回一个ack packet，放入ack Queue中。

## 5  HDFS写流程

![img](D:\BigData\image\70.jpg)

写详细步骤：

```
客户端向NameNode发出写文件请求。
检查是否已存在文件、检查权限。若通过检查，直接先将操作写入EditLog，并返回输出流对象。
（注：WAL，write ahead log，先写Log，再写内存，因为EditLog记录的是最新的HDFS客户端执行所有的写操作。如果后续真实写操作失败了，由于在真实写操作之前，操作就被写入EditLog中了，故EditLog中仍会有记录，我们不用担心后续client读不到相应的数据块，因为在第5步中DataNode收到块后会有一返回确认信息，若没写成功，发送端没收到确认信息，会一直重试，直到成功）
client端按128MB的块切分文件。
client将NameNode返回的分配的可写的DataNode列表和Data数据一同发送给最近的第一个DataNode节点，此后client端和NameNode分配的多个DataNode构成pipeline管道，client端向输出流对象中写数据。client每向第一个DataNode写入一个packet，这个packet便会直接在pipeline里传给第二个、第三个DataNode。
（注：并不是写好一个块或一整个文件后才向后分发）
每个DataNode写完一个块后，会返回确认信息。
（注：并不是每写完一个packet后就返回确认信息，个人觉得因为packet中的每个chunk都携带校验信息，没必要每写一个就汇报一下，这样效率太慢。正确的做法是写完一个block块后，对校验信息进行汇总分析，就能得出是否有块写错的情况发生）
写完数据，关闭输输出流。
发送完成信号给NameNode。
（注：发送完成信号的时机取决于集群是强一致性还是最终一致性，强一致性则需要所有DataNode写完后才向NameNode汇报。最终一致性则其中任意一个DataNode写完后就能单独向NameNode汇报，HDFS一般情况下都是强调强一致性）
```

## 6 HDFS读流程

![HDFS读流程](D:\BigData\image\71.jpg)

读相对于写，简单一些
读详细步骤：

```
client访问NameNode，查询元数据信息，获得这个文件的数据块位置列表，返回输入流对象。
就近挑选一台datanode服务器，请求建立输入流 。
DataNode向输入流中中写数据，以packet为单位来校验。
关闭输入流
```

## 7 读写过程，数据完整性如何保持？

通过校验和。因为每个chunk中都有一个校验位，一个个chunk构成packet，一个个packet最终形成block，故可在block上求校验和。

HDFS 的client端即实现了对 HDFS 文件内容的校验和 (checksum) 检查。当客户端创建一个新的HDFS文件时候，分块后会计算这个文件每个数据块的校验和，此校验和会以一个隐藏文件形式保存在同一个 HDFS 命名空间下。当client端从HDFS中读取文件内容后，它会检查分块时候计算出的校验和（隐藏文件里）和读取到的文件块中校验和是否匹配，如果不匹配，客户端可以选择从其他 Datanode 获取该数据块的副本。

HDFS中文件块目录结构具体格式如下：

${dfs.datanode.data.dir}/
├── current
│ ├── BP-526805057-127.0.0.1-1411980876842
│ │ └── current
│ │ ├── VERSION
│ │ ├── finalized
│ │ │ ├── blk_1073741825
│ │ │ ├── blk_1073741825_1001.meta
│ │ │ ├── blk_1073741826
│ │ │ └── blk_1073741826_1002.meta
│ │ └── rbw
│ └── VERSION
└── in_use.lock

in_use.lock表示DataNode正在对文件夹进行操作
rbw是“replica being written”的意思，该目录用于存储用户当前正在写入的数据。
Block元数据文件（*.meta）由一个包含版本、类型信息的头文件和一系列校验值组成。校验和也正是存在其中。

## 8 NameNode和SecondaryNameNode

### 8.1 NameNode中的元数据是存储在哪里的？

首先，我们做个假设，如果存储在NameNode节点的磁盘中，因为经常需要进行随机访问，还有响应客户请求，必然是效率过低。因此，**<u>元数据需要存放在内存中</u>**。但如果只存在内存中，一旦断电，元数据丢失，整个集群就无法工作了。因此产生在磁盘中备份元数据的FsImage。

这样又会带来新的问题，当在内存中的元数据更新时，如果同时更新FsImage，就会导致效率过低，但如果不更新，就会发生一致性问题，一旦NameNode节点断电，就会产生数据丢失。因此，引入Edits文件(只进行追加操作，效率很高)。每当元数据有更新或者添加元数据时，修改内存中的元数据并追加到Edits中。这样，一旦NameNode节点断电，可以通过FsImage和Edits的合并，合成元数据。

但是，如果长时间添加数据到Edits中，会导致该文件数据过大，效率降低，而且一旦断电，恢复元数据需要的时间过长。因此，需要定期进行FsImage和Edits的合并，如果这个操作由NameNode节点完成，又会效率过低。因此，引入一个新的节点SecondaryNamenode，专门用于FsImage和Edits的合并。

### 8.2 工作机制

<img src="D:\BigData\image\1569415989971.png" alt="1569415989971" style="zoom:150%;" />

1. 第一阶段：NameNode启动

（1）第一次启动NameNode格式化后，创建Fsimage和Edits文件。如果不是第一次启动，直接加载编辑日志和镜像文件到内存。

（2）客户端对元数据进行增删改的请求。

（3）NameNode记录操作日志，更新滚动日志。

（4）NameNode在内存中对数据进行增删改。

2. 第二阶段：Secondary NameNode工作

​       （1）Secondary NameNode询问NameNode是否需要CheckPoint。直接带回NameNode是否检查结果。

​       （2）Secondary NameNode请求执行CheckPoint。

​       （3）NameNode滚动正在写的Edits日志。

​       （4）将滚动前的编辑日志和镜像文件拷贝到Secondary NameNode。

​       （5）Secondary NameNode加载编辑日志和镜像文件到内存，并合并。

​       （6）生成新的镜像文件fsimage.chkpoint。

​       （7）拷贝fsimage.chkpoint到NameNode。

​       （8）NameNode将fsimage.chkpoint重新命名成fsimage。



### 8.3 Fsimage和edits

![1569416218594](D:\BigData\image\1569416218594.png)

## 9 DataNode

### 9.1 工作机制

![1569463478761](D:\BigData\image\1569463478761.png)

1）一个数据块在DataNode上以文件形式存储在磁盘上，包括两个文件，一个是数据本身，一个是元数据包括数据块的长度，块数据的校验和，以及时间戳。

2）DataNode启动后向NameNode注册，通过后，周期性（1小时）的向NameNode上报所有的块信息。

3）心跳是每3秒一次，心跳返回结果带有NameNode给该DataNode的命令如复制块数据到另一台机器，或删除某个数据块。如果超过10分钟30秒没有收到某个DataNode的心跳，则认为该节点不可用。

4）集群运行中可以安全加入和退出一些机器。

### 9.2 数据完整性

1）当DataNode读取Block的时候，它会计算CheckSum。

2）如果计算后的CheckSum，与Block创建时值不一样，说明Block已经损坏。

3）Client读取其他DataNode上的Block。

4）在其文件创建后周期验证

## 10 hadoop服役新节点注意事项

core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml大部分一致，namenode的core-site.xml白名单与ResourceNodeManager的白名单存在切基本一致。slaves包括全部节点（服役或者未服役的）。

# MapReduce

## 1 MR定义

MapReduce是一个分布式运算程序的编程框架，是用户开发“基于Hadoop的数据分析应用”的核心框架。

MapReduce核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发运行在一个Hadoop集群上。

## 2 MR优缺点

### 2.1 优点

1）MapReduce易于编程

它简单的实现一些接口，就可以完成一个分布式程序，这个分布式程序可以分布到大量廉价的PC机器上运行。也就是说你写一个分布式程序，跟写一个简单的串行程序是一模一样的。就是因为这个特点使得MapReduce编程变得非常流行。

2）良好的扩展性

当你的计算资源不能得到满足的时候，你可以通过简单的增加机器来扩展它的计算能力。

3）高容错性

MapReduce设计的初衷就是使程序能够部署在廉价的PC机器上，这就要求它具有很高的容错性。比如其中一台机器挂了，它可以把上面的计算任务转移到另外一个节点上运行，不至于这个任务运行失败，而且这个过程不需要人工参与，而完全是由Hadoop内部完成的。

4）适合PB级以上海量数据的离线处理

### 2.2 缺点

1）不擅长实时计算

MapReduce无法像MySQL一样，在毫秒或者秒级内返回结果。

2）不擅长流式计算

流式计算的输入数据是动态的，而MapReduce的输入数据集是静态的，不能动态变化。这是因为MapReduce自身的设计特点决定了数据源必须是静态的。

3）不擅长DAG（有向图）计算

多个应用程序存在依赖关系，后一个应用程序的输入为前一个的输出。在这种情况下，MapReduce并不是不能做，而是使用后，每个MapReduce作业的输出结果都会写入到磁盘，会造成大量的磁盘IO，导致性能非常的低下。

## 3 常用数据序列化类型

| Java类型 | **Hadoop Writable**类型 |
| -------- | ----------------------- |
| boolean  | BooleanWritable         |
| byte     | ByteWritable            |
| int      | IntWritable             |
| float    | FloatWritable           |
| long     | LongWritable            |
| double   | DoubleWritable          |
| String   | Text                    |
| map      | MapWritable             |
| array    | ArrayWritable           |

## 4 MapReduce框架原理

### 4.1  InputFormat数据输入

#### 4.1.1 片与MapTask并行度决定机制

1．问题引出

MapTask的并行度决定Map阶段的任务处理并发度，进而影响到整个Job的处理速度。

思考：1G的数据，启动8个MapTask，可以提高集群的并发处理能力。那么1K的数据，也启动8个MapTask，会提高集群性能吗？MapTask并行任务是否越多越好呢？哪些因素影响了MapTask并行度？                                                  

2．MapTask并行度决定机制

**数据块：**Block是HDFS物理上把数据分成一块一块。

**数据切片：**数据切片只是在逻辑上对输入进行分片，并不会在磁盘上将其切分成片进行存储。

![1570349313285](D:\BigData\image\1570349313285.png)

#### 4.1.2 Job提交流程源码和切片源码详解

##### 1．Job提交流程源码详解，如图4-8所示

```
waitForCompletion();//驱动提交

submit();

// 1建立连接
	connect();	
		// 1）创建提交Job的代理
		new Cluster(getConfiguration());
			// （1）判断是本地yarn还是远程
			initialize(jobTrackAddr, conf); 

// 2 提交job
submitter.submitJobInternal(Job.this, cluster)
	// 1）创建给集群提交数据的Stag路径
	Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);

	// 2）获取jobid ，并创建Job路径
	JobID jobId = submitClient.getNewJobID();

	// 3）拷贝jar包到集群
copyAndConfigureFiles(job, submitJobDir);	
	rUploader.uploadFiles(job, jobSubmitDir);

// 4）计算切片，生成切片规划文件
writeSplits(job, submitJobDir);
		maps = writeNewSplits(job, jobSubmitDir);
		input.getSplits(job);

// 5）向Stag路径写XML配置文件
writeConf(conf, submitJobFile);
	conf.writeXml(out);

// 6）提交Job,返回提交状态
status = submitClient.submitJob(jobId, submitJobDir.toString(), job.getCredentials());

```

![1570349572472](D:\BigData\image\1570349572472.png)

​																图4-8

2. ##### TextInputFormat切片源码解析

   （1）程序先找到你数据存储的目录。

   （2）开始遍历处理（规划切片）目录下的每一个文件

   （3）遍历第一个文件ss.txt

   ​		a）获取文件大小fs.sizeOf(ss.txt）

   ​		b）计算切片大小

   ​			computeSplitSize(Math.max(minSize,Math.min(maxSize,blocksize)))=blocksize=128M

   ​		c）默认情况下，切片大小=blocksize

   ​		d）开始切，形成第1个切片：ss.txt—0:128M
   第2个切片ss.txt—128:256M
   第3个切片ss.txt—256M:300M（每次切片时，都要判断切完剩下的部分是否大于块的1.1倍，不大于1.1倍就划分一块切片）

   ​		e）将切片信息写到一个切片规划文件中

   ​		f）整个切片的核心过程在getSplit()方法中完成

   ​		g）**InputSplit**只记录了切片的元数据信息，比如起始位置、长度以及所在的节点列表等。

   （4）提交切片规划文件到YARN上，YARN上的MrAppMaster就可以根据切片规划文件计算开启MapTask个数。

   3. ##### CombineTextInputFormat切片机制

      框架默认的TextInputFormat切片机制是对任务按文件规划切片，不管文件多小，都会是一个单独的切片，都会交给一个MapTask，这样如果有大量小文件，就会产生大量的MapTask，处理效率极其低下。

      1、应用场景：

      CombineTextInputFormat用于**小文件过多**的场景，它可以将多个小文件从逻辑上规划到一个切片中，这样，多个小文件就可以交给一个MapTask处理。

      2、虚拟存储切片最大值设置

      CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m

      注意：虚拟存储切片最大值设置最好根据实际的小文件大小情况来设置具体的值。

      3、切片机制

      生成切片过程包括：虚拟存储过程和切片过程二部分。

      ![1570350341306](D:\BigData\image\1570350341306.png)

   

   （1）虚拟存储过程：

   将输入目录下所有文件大小，依次和设置的setMaxInputSplitSize值比较，如果不大于设置的最大值，逻辑上划分一个块。如果输入文件大于设置的最大值且大于两倍，那么以最大值切割一块；当剩余数据大小超过设置的最大值且不大于最大值2倍，此时将文件均分成2个虚拟存储块（防止出现太小切片）。

   例如setMaxInputSplitSize值为4M，输入文件大小为8.02M，则先逻辑上分成一个4M。剩余的大小为4.02M，如果按照4M逻辑划分，就会出现0.02M的小的虚拟存储文件，所以将剩余的4.02M文件切分成（2.01M和2.01M）两个文件。

   （2）切片过程：

   （a）判断虚拟存储的文件大小是否大于setMaxInputSplitSize值，大于等于则单独形成一个切片。

   （b）如果不大于则跟下一个虚拟存储文件进行合并，共同形成一个切片。

   （c）测试举例：有4个小文件大小分别为1.7M、5.1M、3.4M以及6.8M这四个小文件，则虚拟存储之后形成6个文件块，大小分别为：

   1.7M，（2.55M、2.55M），3.4M以及（3.4M、3.4M）

   最终会形成3个切片，大小分别为：

   （1.7+2.55）M，（2.55+3.4）M，（3.4+3.4）M

### 4.2 Shuffle机制

Map方法之后，Reduce方法之前的数据处理过程称之为Shuffle。

![1570350629232](D:\BigData\image\1570350629232.png)

#### 4.2.1 分区

（1）如果ReduceTask的数量> getPartition的结果数，则会多产生几个空的输出文件part-r-000xx；

（2）如果1<ReduceTask的数量<getPartition的结果数，则有一部分分区数据无处安放，会Exception；

（3）如果ReduceTask的数量=1，则不管MapTask端输出多少个分区文件，最终结果都交给这一个ReduceTask，最终也就只会产生一个结果文件 part-r-00000；

（4）分区号必须从零开始，逐一累加。

4.2.2 Combiner合并

（1）Combiner是MR程序中Mapper和Reducer之外的一种组件。

（2）Combiner组件的父类就是Reducer。

（3）Combiner和Reducer的区别在于运行的位置

Combiner是在每一个MapTask所在的节点运行;

Reducer是接收全局所有Mapper的输出结果；

（4）Combiner的意义就是对每一个MapTask的输出进行局部汇总，以减小网络传输量。

（5）Combiner能够应用的前提是不能影响最终的业务逻辑，而且，Combiner的输出kv应该跟Reducer的输入kv类型要对应起来。

## 5 MR工作机制

### 5.1 MapTask工作机制

![1570351254736](D:\BigData\image\1570351254736.png)

​		（1）Read阶段：MapTask通过用户编写的RecordReader，从输入InputSplit中解析出一个个key/value。

​       （2）Map阶段：该节点主要是将解析出的key/value交给用户编写map()函数处理，并产生一系列新的key/value。

​       （3）Collect收集阶段：在用户编写map()函数中，当数据处理完成后，一般会调用OutputCollector.collect()输出结果。在该函数内部，它会将生成的key/value分区（调用Partitioner），并写入一个环形内存缓冲区中。

​       （4）Spill阶段：即“溢写”，当环形缓冲区满后，MapReduce会将数据写到本地磁盘上，生成一个临时文件。需要注意的是，将数据写入本地磁盘之前，先要对数据进行一次本地排序，并在必要时对数据进行合并、压缩等操作。

​       溢写阶段详情：

​       步骤1：利用快速排序算法对缓存区内的数据进行排序，排序方式是，先按照分区编号Partition进行排序，然后按照key进行排序。这样，经过排序后，数据以分区为单位聚集在一起，且同一分区内所有数据按照key有序。

​       步骤2：按照分区编号由小到大依次将每个分区中的数据写入任务工作目录下的临时文件output/spillN.out（N表示当前溢写次数）中。如果用户设置了Combiner，则写入文件之前，对每个分区中的数据进行一次聚集操作。

​       步骤3：将分区数据的元信息写到内存索引数据结构SpillRecord中，其中每个分区的元信息包括在临时文件中的偏移量、压缩前数据大小和压缩后数据大小。如果当前内存索引大小超过1MB，则将内存索引写到文件output/spillN.out.index中。

​       （5）Combine阶段：当所有数据处理完成后，MapTask对所有临时文件进行一次合并，以确保最终只会生成一个数据文件。

​       当所有数据处理完后，MapTask会将所有临时文件合并成一个大文件，并保存到文件output/file.out中，同时生成相应的索引文件output/file.out.index。

​       在进行文件合并过程中，MapTask以分区为单位进行合并。对于某个分区，它将采用多轮递归合并的方式。每轮合并io.sort.factor（默认10）个文件，并将产生的文件重新加入待合并列表中，对文件排序后，重复以上过程，直到最终得到一个大文件。

​       让每个MapTask最终只生成一个数据文件，可避免同时打开大量文件和同时读取大量小文件产生的随机读取带来的开销。

### 5.2 ReduceTask工作机制

![1570351613857](D:\BigData\image\1570351613857.png)

​       （1）Copy阶段：ReduceTask从各个MapTask上远程拷贝一片数据，并针对某一片数据，如果其大小超过一定阈值，则写到磁盘上，否则直接放到内存中。

​       （2）Merge阶段：在远程拷贝数据的同时，ReduceTask启动了两个后台线程对内存和磁盘上的文件进行合并，以防止内存使用过多或磁盘上文件过多。

​       （3）Sort阶段：按照MapReduce语义，用户编写reduce()函数输入数据是按key进行聚集的一组数据。为了将key相同的数据聚在一起，Hadoop采用了基于排序的策略。由于各个MapTask已经实现对自己的处理结果进行了局部排序，因此，ReduceTask只需对所有数据进行一次归并排序即可。

​       （4）Reduce阶段：reduce()函数将计算结果写到HDFS上。

## 6 数据压缩

### 6.1 概述

压缩技术能够有效减少底层存储系统（HDFS）读写字节数。压缩提高了网络带宽和磁盘空间的效率。在运行MR程序时，I/O操作、网络数据传输、 Shuffle和Merge要花大量的时间，尤其是数据规模很大和工作负载密集的情况下，因此，使用数据压缩显得非常重要。

鉴于磁盘I/O和网络带宽是Hadoop的宝贵资源，数据压缩对于节省资源、最小化磁盘I/O和网络传输非常有帮助。可以在任意MapReduce阶段启用压缩。不过，尽管压缩与解压操作的CPU开销不高，其性能的提升和资源的节省并非没有代价。

### 6.2 MR支持的压缩编码

| 压缩格式 | hadoop自带？ | 算法    | 文件扩展名 | 是否可切分 | 换成压缩格式后，原来的程序是否需要修改 |
| -------- | ------------ | ------- | ---------- | ---------- | -------------------------------------- |
| DEFLATE  | 是，直接使用 | DEFLATE | .deflate   | 否         | 和文本处理一样，不需要修改             |
| Gzip     | 是，直接使用 | DEFLATE | .gz        | 否         | 和文本处理一样，不需要修改             |
| bzip2    | 是，直接使用 | bzip2   | .bz2       | 是         | 和文本处理一样，不需要修改             |
| LZO      | 否，需要安装 | LZO     | .lzo       | 是         | 需要建索引，还需要指定输入格式         |
| Snappy   | 否，需要安装 | Snappy  | .snappy    | 否         | 和文本处理一样，不需要修改             |

为了支持多种压缩/解压缩算法，Hadoop引入了编码/解码器，如下表所示。

| 压缩格式 | 对应的编码/解码器                          |
| -------- | ------------------------------------------ |
| DEFLATE  | org.apache.hadoop.io.compress.DefaultCodec |
| gzip     | org.apache.hadoop.io.compress.GzipCodec    |
| bzip2    | org.apache.hadoop.io.compress.BZip2Codec   |
| LZO      | com.hadoop.compression.lzo.LzopCodec       |
| Snappy   | org.apache.hadoop.io.compress.SnappyCodec  |

压缩性能的比较

| 压缩算法 | 原始文件大小 | 压缩文件大小 | 压缩速度 | 解压速度 |
| -------- | ------------ | ------------ | -------- | -------- |
| gzip     | 8.3GB        | 1.8GB        | 17.5MB/s | 58MB/s   |
| bzip2    | 8.3GB        | 1.1GB        | 2.4MB/s  | 9.5MB/s  |
| LZO      | 8.3GB        | 2.9GB        | 49.3MB/s | 74.6MB/s |

### 6.3 压缩方式选择

#### 6.3.1 Gzip压缩

优点：压缩率比较高，而且压缩/解压速度也比较快；Hadoop本身支持，在应用中处理Gzip格式的文件就和直接处理文本一样；大部分Linux系统都自带Gzip命令，使用方便。

缺点：不支持Split。

应用场景：当每个文件压缩之后在130M以内的（1个块大小内），都可以考虑用Gzip压缩格式。例如说一天或者一个小时的日志压缩成一个Gzip文件。

#### 6.3.2 Bzip2压缩

优点：支持Split；具有很高的压缩率，比Gzip压缩率都高；Hadoop本身自带，使用方便。

缺点：压缩/解压速度慢。

应用场景：适合对速度要求不高，但需要较高的压缩率的时候；或者输出之后的数据比较大，处理之后的数据需要压缩存档减少磁盘空间并且以后数据用得比较少的情况；或者对单个很大的文本文件想压缩减少存储空间，同时又需要支持Split，而且兼容之前的应用程序的情况。

#### 6.3.3 Lzo压缩

优点：压缩/解压速度也比较快，合理的压缩率；支持Split，是Hadoop中最流行的压缩格式；可以在Linux系统下安装lzop命令，使用方便。

缺点：压缩率比Gzip要低一些；Hadoop本身不支持，需要安装；在应用中对Lzo格式的文件需要做一些特殊处理（为了支持Split需要建索引，还需要指定InputFormat为Lzo格式）。

应用场景：一个很大的文本文件，压缩之后还大于200M以上的可以考虑，而且单个文件越大，Lzo优点越越明显。

#### 6.3.4 Snappy压缩

优点：高速压缩速度和合理的压缩率。

缺点：不支持Split；压缩率比Gzip要低；Hadoop本身不支持，需要安装。

应用场景：当MapReduce作业的Map输出的数据比较大的时候，作为Map到Reduce的中间数据的压缩格式；或者作为一个MapReduce作业的输出和另外一个MapReduce作业的输入。

### 6.4 压缩参数配置

| 参数                                                         | 默认值                                                       | 阶段        | 建议                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ----------- | --------------------------------------------- |
| io.compression.codecs      （在core-site.xml中配置）         | org.apache.hadoop.io.compress.DefaultCodec,   org.apache.hadoop.io.compress.GzipCodec,   org.apache.hadoop.io.compress.BZip2Codec | 输入压缩    | Hadoop使用文件扩展名判断是否支持某种编解码器  |
| mapreduce.map.output.compress（在mapred-site.xml中配置）     | false                                                        | mapper输出  | 这个参数设为true启用压缩                      |
| mapreduce.map.output.compress.codec（在mapred-site.xml中配置） | org.apache.hadoop.io.compress.DefaultCodec                   | mapper输出  | 企业多使用LZO或Snappy编解码器在此阶段压缩数据 |
| mapreduce.output.fileoutputformat.compress（在mapred-site.xml中配置） | false                                                        | reducer输出 | 这个参数设为true启用压缩                      |
| mapreduce.output.fileoutputformat.compress.codec（在mapred-site.xml中配置） | org.apache.hadoop.io.compress.   DefaultCodec                | reducer输出 | 使用标准工具或者编解码器，如gzip和bzip2       |
| mapreduce.output.fileoutputformat.compress.type（在mapred-site.xml中配置） | RECORD                                                       | reducer输出 | SequenceFile输出使用的压缩类型：NONE和BLOCK   |

# yarn

## 1 概述

Yarn是一个资源调度平台，负责为运算程序提供服务器运算资源，相当于一个分布式的操作系统平台，而MapReduce等运算程序则相当于运行于操作系统之上的应用程序。

YARN主要由ResourceManager、NodeManager、ApplicationMaster和Container等组件构成

![1570352574547](D:\BigData\image\1570352574547.png)

## 2 yarn工作机制

![1570352805127](D:\BigData\image\1570352805127.png)

​       （1）MR程序提交到客户端所在的节点。

​       （2）YarnRunner向ResourceManager申请一个Application。

​       （3）RM将该应用程序的资源路径返回给YarnRunner。

​       （4）该程序将运行所需资源提交到HDFS上。

​       （5）程序资源提交完毕后，申请运行mrAppMaster。

​       （6）RM将用户的请求初始化成一个Task。

​       （7）其中一个NodeManager领取到Task任务。

​       （8）该NodeManager创建容器Container，并产生MRAppmaster。

​       （9）Container从HDFS上拷贝资源到本地。

​       （10）MRAppmaster向RM 申请运行MapTask资源。

​       （11）RM将运行MapTask任务分配给另外两个NodeManager，另两个NodeManager分别领取任务并创建容器。

​       （12）MR向两个接收到任务的NodeManager发送程序启动脚本，这两个NodeManager分别启动MapTask，MapTask对数据分区排序。

（13）MrAppMaster等待所有MapTask运行完毕后，向RM申请容器，运行ReduceTask。

​       （14）ReduceTask向MapTask获取相应分区的数据。

​       （15）程序运行完毕后，MR会向RM申请注销自己。

## 3 资源调度器

目前，Hadoop作业调度器主要有三种：FIFO、Capacity Scheduler和Fair Scheduler。Hadoop2.7.2默认的资源调度器是Capacity Scheduler。

### 3.1 先进先出调度器（FIFO）

![1570353087226](D:\BigData\image\1570353087226.png)

### 3.2  容量调度器（Capacity Scheduler）

![1570353226555](D:\BigData\image\1570353226555.png)

Capacity调度器说的通俗点，可以理解成一个个的资源队列。这个资源队列是用户自己去分配的。比如我大体上把整个集群分成了AB两个队列，A队列给A项目组的人来使用。B队列给B项目组来使用。但是A项目组下面又有两个方向，那么还可以继续分，比如专门做BI的和做实时分析的。那么队列的分配就可以参考下面的树形结构：

```
root
------a[60%]
      |---a.bi[40%]
      |---a.realtime[60%]
------b[40%]
```

a队列占用整个资源的60%，b队列占用整个资源的40%。a队列里面又分了两个子队列，一样也是2:3分配。

虽然有了这样的资源分配，但是并不是说a提交了任务，它就只能使用60%的资源，那40%就空闲着。只要资源实在空闲状态，那么a就可以使用100%的资源。但是一旦b提交了任务，a就需要在释放资源后，把资源还给b队列，直到ab平衡在3:2的比例。

粗粒度上资源是按照上面的方式进行，在每个队列的内部，还是按照FIFO的原则来分配资源的。

### 3.3 公平调度器（Fair Scheduler）

![1570353649441](D:\BigData\image\1570353649441.png)

## 4 任务的推测执行

### 4.1 推测执行机制

发现拖后腿的任务，比如某个任务运行速度远慢于任务平均速度。为拖后腿任务启动一个备份任务，同时运行。谁先运行完，则采用谁的结果。

### 4.2 执行推测任务的前提条件

（1）每个Task只能有一个备份任务

（2）当前Job已完成的Task必须不小于0.05（5%）

（3）开启推测执行参数设置。mapred-site.xml文件中默认是打开的。

### 4.3 不能启用推测执行机制情况

   （1）任务间存在严重的负载倾斜；

   （2）特殊任务，比如任务向数据库中写数据。
