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

![未命名文件 (15)](D:\360极速浏览器下载\未命名文件 (15).png)



WordSortReducer流程图：

![未命名文件 (16)](D:\360极速浏览器下载\未命名文件 (16).png)

**MR过程示意图：**

![未命名文件 (18)](D:\360极速浏览器下载\未命名文件 (18).png)

## 二、实验过程和实验结果

### 1、实验过程截图

（1）在本地hdfs的input文件夹内上传莎士比亚文集txt和停词表、标点表，在hadoop单机伪分布式集群中调用IDEA打包的jar包，其中参数依次为input output 停用表 标点表。

![image-20211020113610437](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020113610437.png)

（2）运行成功：

job1:![image-20211020113652492](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020113652492.png)

job2:![image-20211020103852692](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020103852692.png)

web页面运行成功截图：

![image-20211020103745343](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020103745343.png)

![image-20211020103805200](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020103805200.png)

### 2、实验结果展示与分析

结果按照题目示例格式，将词频排序按照文件名排序输出在一个结果文件中（注：由于虚拟机中未安装中文输入法，因此"总计"写作"all"进行统计展示。)

![image-20211020114537993](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020114537993.png)



输出结果截图如下：

![image-20211020114104205](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020114104205.png)

![image-20211020113954215](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020113954215.png)



## 三、实验不足和改进之处

#### 1.性能优化

注：此部分改进添加进入了源代码

**存在不足：**本任务job1和job2的运行时间存在较大差距，在本机伪分布式上运行时间分别为7分20秒和50秒。即wordcount任务对于处理多个文件词频统计效率较低，我对于mapreduce内置版wordcount进行测试，测试时间为5分钟。

**原因：**wordcount的map将每一个单词独立发送给reduce，在文本量大时网络传输负担较大，同时占有较大存储空间，特别是在本作业中我为了分别统计文件和总量，对每个单词都进行了两次输出，因此对内存消耗更高，需要进一步优化。

**解决方案：**利用combiner操作，对本地map输出进行基础合并，减轻reduce的负担和网络通讯负担。因为combine和reduce逻辑相同，对reduce进行改写。

**优化结果**：

（1）运行时间：

![image-20211020172117134](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020172117134.png)

wordcount中job1的运行时间由7分20秒减少到接近5分钟，时间明显减少。

（2）运行分析：

优化前reduce input records为844620，而优化后减少到245838，combiner的使用显著减少了网络传输和reduce的负担，提高了任务运行性能。

![image-20211020172427987](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020172427987.png)

![image-20211020172529044](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020172529044.png)

![image-20211020172503268](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020172503268.png)



#### 2.接口优化（提高可扩展性）

注：此部分添加到源代码中，如果不在调用时采用第五位输入，依然输出前100.

**存在不足**：目前wordcount3.0程序针对题目要求，进行maxrank和大小写、数字、停用词、标点及字符长度的限制，未向使用者开放除停用词表和标点表以外的接口，同时可能用户自己无法提供停用词表和标点表，应用范围较小，可扩展性较差。

**解决方案：**提供一种默认调用方式，设置系统自带的停用词表和标点表，如果用户不输入参数则为默认格式。向用户开放maxrank/大小写/数字/停用词/标点/字符长度的是否选择和输入接口，并在程序中通过接口调用实现用户期待的输出。（这部分由于时间限制，只对maxrank的设置实现，在第五位参数进行输入，通过conf进行传递。其他实现原理相似，故不展开实验）

**实现效果**：

![image-20211020185131136](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020185131136.png)

![image-20211020185154701](C:\Users\Administrator\AppData\Roaming\Typora\typora-user-images\image-20211020185154701.png)