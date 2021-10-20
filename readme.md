# 作业5：莎士比亚文集词频统计3.0 

## 一、设计思路

### 1、实验说明：

对若干莎士比亚文集txt文件数据进行处理，分别对每个文档和所有文档统计除停用词和标点、数字以外长度大于3的单词词频，并排序输出前100位。

### 2.设计概述及流程图、map/reduce过程示意图

#### 2.1设计概述

**主程序**：WordCount Class 对参数进行处理，将停词表和标点表读入分布式内存，将任务分为两项job依次执行，分别进行优化版词频统计（WordCombiner)和词频排序输出(WordSort)。

**优化版词频统计**：与wordcount2.0采取相同的设计思路，map读取分布式缓存中的停词表和标点表，并进行题目要求的数字、大小写和单词长度处理,结束输出格式为<word_file，value>，并另外输出<word_all，value>实现合并输出。输出结果存放在临时文件夹（output/tmp）中，在所有任务结束后删除。

**词频排序输出：**参考倒排索引优化版的设计思路，设计myclass数据类型，存储word、file和value，并设计排序逻辑（file-value（倒序）-word，其中file=="all"排序在最后）。map读取job1结果后存入myclass，shuffle实现排序后由reduce处理，reduce采用Prev和now两个变量控制分隔符和文件名的输出（存放至output/result）。



#### 2.2 设计图

**流程图**：

整体流程图：

![](https://img.xiumi.us/xmi/ua/2p1St/i/0c5bd4801a51927c871391d103d9423c-sz_31842.png?x-oss-process=style/xmwebp)



WordSortReducer流程图：

![](https://img.xiumi.us/xmi/ua/2p1St/i/779d3eee7ec32914ecb1b8ba238f24a6-sz_45304.png?x-oss-process=style/xmwebp)

**MR过程示意图：**

![](https://img.xiumi.us/xmi/ua/2p1St/i/b1ff6e3af41591e5fb61162aabf272b6-sz_100918.png?x-oss-process=style/xmwebp)

## 二、实验过程和实验结果

### 1、实验过程截图

（1）在本地hdfs的input文件夹内上传莎士比亚文集txt和停词表、标点表，在hadoop单机伪分布式集群中调用IDEA打包的jar包，其中参数依次为input output 停用表 标点表。

![](https://img.xiumi.us/xmi/ua/2p1St/i/e9434886a793b8a503b1482968809342-sz_31710.png?x-oss-process=style/xmwebp)

（2）运行成功：

job1:![](https://img.xiumi.us/xmi/ua/2p1St/i/426b06ad9e167d532b093e7d37f23b20-sz_148391.png?x-oss-process=style/xmwebp)

job2:![](https://img.xiumi.us/xmi/ua/2p1St/i/716c68fc106e5fbbf042e3b23fe46fcb-sz_154173.png?x-oss-process=style/xmwebp)

web页面运行成功截图：

![](https://img.xiumi.us/xmi/ua/2p1St/i/fe9ab17f69787ec180f4b3580c05b560-sz_35257.png?x-oss-process=style/xmwebp)

![](https://img.xiumi.us/xmi/ua/2p1St/i/06329e863478fd21816d87c3dc1cc28e-sz_42312.png?x-oss-process=style/xmwebp)

### 2、实验结果展示与分析

结果按照题目示例格式，将词频排序按照文件名排序输出在一个结果文件中（注：由于虚拟机中未安装中文输入法，因此"总计"写作"all"进行统计展示。)

![](https://img.xiumi.us/xmi/ua/2p1St/i/e5d78b9da82160a16b00a9920322d7aa-sz_10321.png?x-oss-process=style/xmwebp)



输出结果截图如下：

![](https://img.xiumi.us/xmi/ua/2p1St/i/5a652cc939b2015e652a968de46d3b59-sz_49180.png?x-oss-process=style/xmwebp)

![](https://img.xiumi.us/xmi/ua/2p1St/i/ea71b7765ae546b1f3f94b97077ba2fd-sz_28614.png?x-oss-process=style/xmwebp)



## 三、实验不足和改进之处

#### 1.性能优化

注：此部分改进添加进入了源代码

**存在不足**：本任务job1和job2的运行时间存在较大差距，在本机伪分布式上运行时间分别为7分20秒和50秒。即wordcount任务对于处理多个文件词频统计效率较低，我对于mapreduce内置版wordcount进行测试，测试时间为5分钟。

**原因**：wordcount的map将每一个单词独立发送给reduce，在文本量大时网络传输负担较大，同时占有较大存储空间，特别是在本作业中我为了分别统计文件和总量，对每个单词都进行了两次输出，因此对内存消耗更高，需要进一步优化。

**解决方案**：利用combiner操作，对本地map输出进行基础合并，减轻reduce的负担和网络通讯负担。因为combine和reduce逻辑相同，对reduce进行改写。

**优化结果**：

（1）运行时间：

![](https://img.xiumi.us/xmi/ua/2p1St/i/ed98476e372f43c2370312c1d2b50cf1-sz_33276.png?x-oss-process=style/xmwebp)

wordcount中job1的运行时间由7分20秒减少到接近5分钟，时间明显减少。

（2）运行分析：

优化前reduce input records为844620，而优化后减少到245838，combiner的使用显著减少了网络传输和reduce的负担，提高了任务运行性能。

![](https://img.xiumi.us/xmi/ua/2p1St/i/414bcb76de644497cd456d3d57522cd8-sz_10059.png?x-oss-process=style/xmwebp)

![](https://img.xiumi.us/xmi/ua/2p1St/i/ff04dd18232fbcde40585bed453c5856-sz_10343.png?x-oss-process=style/xmwebp)

![](https://img.xiumi.us/xmi/ua/2p1St/i/62de6b2c99cbc21f94831ff23a0c1a71-sz_10143.png?x-oss-process=style/xmwebp)



#### 2.接口优化（提高可扩展性）

注：此部分添加到源代码中，如果不在调用时采用第五位输入，依然输出前100.

**存在不足**：目前wordcount3.0程序针对题目要求，进行maxrank和大小写、数字、停用词、标点及字符长度的限制，未向使用者开放除停用词表和标点表以外的接口，同时可能用户自己无法提供停用词表和标点表，应用范围较小，可扩展性较差。

**解决方案：**提供一种默认调用方式，设置系统自带的停用词表和标点表，如果用户不输入参数则为默认格式。向用户开放maxrank/大小写/数字/停用词/标点/字符长度的是否选择和输入接口，并在程序中通过接口调用实现用户期待的输出。（这部分由于时间限制，只对maxrank的设置实现，在第五位参数进行输入，通过conf进行传递。其他实现原理相似，故不展开实验）

**实现效果**：

![](https://img.xiumi.us/xmi/ua/2p1St/i/6dd8d7f35f38933ecc01cec1d9cb5775-sz_27263.png?x-oss-process=style/xmwebp)

![](https://img.xiumi.us/xmi/ua/2p1St/i/6bc0a0cf5a6bf6bd3ece19ccb6ebfcf7-sz_55969.png?x-oss-process=style/xmwebp)
