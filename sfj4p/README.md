java4p

本jar包实现了在java中运行spark的过程中，支持用python程序处理中间数据。

本jar包的入口函数是 PythonCompute 类的 compute(String fileUri, Dataset<Row> input) 函数:
  参数：
    String fileUri：
      用户上传代码zip包的位置，支持本地文件系统和hdfs系统两种链接格式。
      其中本地文件系统链接格式为： file:/home/python/python.zip
      hdfs系统链接格式为： hdfs:/192.168.0.110:55070/htsc/namespaces/default/artifacts/python/python.zip
    Dataset<Row> input：
      spark过程数据，作为参数输入到python中
  返回值：
    JavaPairRDD<StructType, Row>:
      python程序运行完成后的结果数据，正常情况下每一个RDD的StructType都是一样的，不同的只是Row里的数据。

注意事项：
  1. 运行的所有机器必须安装python3版本，并且命令行指令都是python（而不是python3）。
  2. zip包中必须包含 pythonCompute.py 文件，并且存在于zip包的最外层目录。
  3. python程序的入口函数有所规定，必须是 pythonCompute.py 文件的 compute(rddList) 函数，
     其中rddList是Row类型的list。且此函数自然也会是程序的出口，其返回值也必须是Row类型的list。
  4. 用户提交的python程序中用到的所有python包，都必须事先在各个节点上安装好。
