This library adds `withField`, `withFieldRenamed`, and `dropFields` methods to the Column class allowing users to easily add, rename, and drop fields inside StructType columns. 
The signature and behaviour of these methods is intended to be similar to their Dataset equivalents, namely the `withColumn`, `withColumnRenamed`, and `drop` methods.

The methods themselves are backed by efficient Catalyst Expressions and as a result, should provide better performance than equivalent UDFs. 
While this library "monkey patches" the methods on to the Column class, 
there is an on-going effort to add these methods natively to the Column class in the Apache Spark SQL project. 
You can follow along with the progress of this initiative in [SPARK-22231](https://issues.apache.org/jira/browse/SPARK-22231).

If you find this project useful, please consider supporting it by giving a star!

# Supported Spark versions

MSE should work without any further requirements on Spark/PySpark 2.4.x. 
The library is available for Python 3.x.

# Installation

Stable releases of MSE are published to PyPi.
You will also need to provide your PySpark application/s with the path to the MSE jar which you can get from [here](https://search.maven.org/artifact/com.github.fqaiser94/mse_2.11).  
For example: 

```bash
pip install mse
curl https://repo1.maven.org/maven2/com/github/fqaiser94/mse_2.11/0.2.2/mse_2.11-0.2.2.jar --output mse.jar
pyspark --jars mse.jar
```

If you get errors like `TypeError: 'JavaPackage' object is not callable`, this usually indicates that you haven't 
provided PySpark with the correct path to the MSE jar.

# Usage 
To bring in to scope the (implicit) Column methods in Python, use:

```python3
from mse import *
```

You can now use these methods to manipulate fields in a StructType column: 

```python3
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mse import *

# Generate some example data
structLevel1 = spark.createDataFrame(
  sc.parallelize([Row(Row(1, None, 3))]),
  StructType([
    StructField("a", StructType([
      StructField("a", IntegerType()),
      StructField("b", IntegerType()),
      StructField("c", IntegerType())]))])).cache()
      
structLevel1.show()
# +-------+                                                                       
# |      a|
# +-------+
# |[1,, 3]|
# +-------+

structLevel1.printSchema()
#  root
#   |-- a: struct (nullable = true)
#   |    |-- a: integer (nullable = true)
#   |    |-- b: integer (nullable = true)
#   |    |-- c: integer (nullable = true)

#  add new field to top level struct
structLevel1.withColumn("a", col("a").withField("d", lit(4))).show()
#  +----------+
#  |         a|
#  +----------+
#  |[1,, 3, 4]|
#  +----------+

#  replace field in top level struct
structLevel1.withColumn("a", col("a").withField("b", lit(2))).show()
#  +---------+
#  |        a|
#  +---------+
#  |[1, 2, 3]|
#  +---------+

#  rename field in top level struct
structLevel1.withColumn("a", col("a").withFieldRenamed("b", "z")).printSchema()
#  root
#   |-- a: struct (nullable = true)
#   |    |-- a: integer (nullable = true)
#   |    |-- z: integer (nullable = true)
#   |    |-- c: integer (nullable = true)

#  drop field in top level struct
structLevel1.withColumn("a", col("a").dropFields("b")).show()
#  +------+
#  |     a|
#  +------+
#  |[1, 3]|
#  +------+
```

For more complicated examples, see the GitHub page. 