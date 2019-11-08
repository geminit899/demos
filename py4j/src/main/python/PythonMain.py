def computeNumpy(rdd, emitter):
    import numpy as np
    import pandas as pd
    import random
    result = rdd.map(lambda x:np.empty([int(x), 2], dtype = int))
    outputPath = '/home/rdds/' + str(random.randint(99, 999999))
    result.saveAsTextFile('file://' + outputPath)
    emitter.emit(outputPath)

def computePandas(rdd, emitter):
    import pandas as pd
    d = {'one': [1, 2, 3], 'two': [4, 5, 6]}
    df = pd.DataFrame(d, index = ['a', 'b', 'c'])
    print(df.columns)
    print(df.values)

from pyspark import SparkContext, SparkConf

class Main(object):
    sc = None

    def initSparkContext(self):
        conf = SparkConf().setAppName("zpsb").setMaster("local")
        self.sc = SparkContext(conf = conf)

    def toPython(self, rddPath, emitter):
        rdd = self.sc.textFile("file://" + rddPath, 1)
        computeNumpy(rdd, emitter)

    class Java:
        implements = ["py4j.examples.MainInterface"]

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

main = Main()
gateway = ClientServer(
    java_parameters = JavaParameters(),
    python_parameters = PythonParameters(),
    python_server_entry_point = main
)