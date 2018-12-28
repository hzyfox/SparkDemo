# SparkDemo
Bayes包下面实现了基于Bayes文本分类的多项式模型算法
该包包含三个类
1. TrainSetGenerator 接受4个参数 用于生成训练文件和测试文件到指定目录
>     System.err.println("该生成器生成两个训练文件到训练子目录，生成一个测试文件到测试子目录")<br>
>     System.err.println("该生成器器需要一个父目录、测试文件子目录和两个训练文件子目录 请指定父目录 测试文件子目录 以及两个训练文件子目录")<br>
>     System.err.println("please input hdfs path: <parentPath> <authPath> <data0Path> <data1Path>")<br>
>     System.err.println("路径请以/结尾 开头不要带/")<br>
>     System.err.println("eg. file:/home/admin/data/ auth/ china/ japan/")<br>
>     System.err.println("eg. hdfs:/home/admin/data/ auth/ china/ japan/")<br>

2. BayesBasedOnWord 接受3个参数 用于训练模型
>      System.err.println("该训练器需要一个父目录、两个训练文件子目录 请指定父目录 两个训练子目录")<br>
>      System.err.println("please input hdfs path: <parentPath>  <data0Path> <data1Path>")<br>
>      System.err.println("路径请以/结尾 开头不要带/")<br>
>      System.err.println("eg. file:/home/admin/data/  china/ japan/")<br>
>      System.err.println("eg. hdfs:/home/admin/data/  china/ japan/")<br>

3. PredictFromModel 用已经训练好的模型预测，结果会输出到标准输出
>      System.err.println("该预测器需要一个父目录、测试文件子目录和两个训练文件子目录 请指定父目录 测试文件子目录 以及两个训练文件子目录")<br>
>      System.err.println("please input hdfs path: <parentPath> <authPath> <data0Path> <data1Path>")<br>
>      System.err.println("路径请以/结尾 开头不要带/")<br>
>      System.err.println("eg. file:/home/admin/data/ auth/ china/ japan/")<br>
>      System.err.println("eg. hdfs:/home/admin/data/ auth/ china/ japan/")<br>

4. 编译脚本为build.sh，可以生成去除依赖的jar包

5. 要在Idea里面运行 请将compile-dep profile打开，否则idea运行时无法找到相关依赖，默认是provided-dep,运行时不包含依赖
