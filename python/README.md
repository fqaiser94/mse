# Usage

The python library is a work-in-progress. 
We are actively working developing a proper module and easy pip installation instructions.
For now, you can try following the example below to access the MSE methods. 

```bash
pyspark --driver-class-path ./target/scala-2.11/mse_2.11-0.1.5.jar
```

```python
sc.addPyFile("mse/methods.py")

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from mse.methods import *

structLevel1 = spark.createDataFrame(
  sc.parallelize([Row(Row(1, None, 3))]),
  StructType([
    StructField("a", StructType([
      StructField("a", IntegerType()),
      StructField("b", IntegerType()),
      StructField("c", IntegerType())]))])).cache()

structLevel1.withColumn("a", col("a").withField("d", lit(100))).show()
structLevel1.withColumn("a", col("a").withFieldRenamed("a", "z")).printSchema()
structLevel1.withColumn("a", col("a").dropFields("c", "b")).show()

structLevel1.withColumn("a", add_struct_field("a", "d", lit(12345))).show()
```