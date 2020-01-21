# Make Structs Easy (MSE)

This library adds `withField`, `withFieldRenamed`, and `dropFields` (implicit) methods to the Column class to enable easy StructType column manipulation. 
The signature and behaviour of these methods is meant to be similar to their Dataset equivalents, namely the `withColumn`, `withColumnRenamed`, and `drop` methods.
The methods themselves have been implemented using Catalyst Expressions and so should provide good performance (and certainly better than UDFs). 

# Supported Spark versions
MSE should work without any further requirements on Spark 2.4.x

# Usage 

Start a spark-shell session. Make sure to include the correct jar version.  

```bash
spark-shell --jars mse_2.11-0.1.jar
``` 

To bring in to scope the (implicit) Column methods, use:   

```scala
import com.mse.column.methods._
```

You can now use these method to manipulate fields in a top-level StructType column: 

```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._

// Generate some example data
val structLevel1 = spark.createDataFrame(sc.parallelize(
  Row(Row(1, null, 3)) :: Nil),
  StructType(Seq(
    StructField("a", StructType(Seq(
      StructField("a", IntegerType),
      StructField("b", IntegerType),
      StructField("c", IntegerType))))))).cache
      
structLevel1.show
// +-------+                                                                       
// |      a|
// +-------+
// |[1,, 3]|
// +-------+

structLevel1.printSchema
// root
//  |-- a: struct (nullable = true)
//  |    |-- a: integer (nullable = true)
//  |    |-- b: integer (nullable = true)
//  |    |-- c: integer (nullable = true)

// add new field to top level struct
structLevel1.withColumn("a", $"a".withField("d", lit(4))).show
// +----------+
// |         a|
// +----------+
// |[1,, 3, 4]|
// +----------+

// replace field in top level struct
structLevel1.withColumn("a", $"a".withField("b", lit(2))).show
// +---------+
// |        a|
// +---------+
// |[1, 2, 3]|
// +---------+

// rename field in top level struct
structLevel1.withColumn("a", $"a".withFieldRenamed("b", "z")).printSchema
// root
//  |-- a: struct (nullable = true)
//  |    |-- a: integer (nullable = true)
//  |    |-- z: integer (nullable = true)
//  |    |-- c: integer (nullable = true)

// drop field in top level struct
structLevel1.withColumn("a", $"a".dropFields("b")).show
// +------+
// |     a|
// +------+
// |[1, 3]|
// +------+

```

You can also use these method to manipulate fields in deeply nested StructType columns: 

```scala
// Generate some example data  
val structLevel2 = spark.createDataFrame(sc.parallelize(
    Row(Row(Row(1, null, 3))) :: Nil),
    StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("a", StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))))))))).cache
          
structLevel2.show
// +---------+
// |        a|
// +---------+
// |[[1,, 3]]|
// +---------+

structLevel2.printSchema
// |-- a: struct (nullable = true)
// |    |-- a: struct (nullable = true)
// |    |    |-- a: integer (nullable = true)
// |    |    |-- b: integer (nullable = true)
// |    |    |-- c: integer (nullable = true)

// add new field to nested struct
structLevel2.withColumn("a", $"a".withField(
  "a", $"a.a".withField(
    "d", lit(4)))).show
// +------------+
// |           a|
// +------------+
// |[[1,, 3, 4]]|
// +------------+


// replace field in nested struct
structLevel2.withColumn("a", $"a".withField(
  "a", $"a.a".withField(
    "b", lit(2)))).show
// +-----------+
// |          a|
// +-----------+
// |[[1, 2, 3]]|
// +-----------+
    
// rename field in nested struct
structLevel2.withColumn("a", $"a".withField(
  "a", $"a.a".withFieldRenamed("b", "z"))).printSchema
// |-- a: struct (nullable = true)
// |    |-- a: struct (nullable = true)
// |    |    |-- a: integer (nullable = true)
// |    |    |-- z: integer (nullable = true)
// |    |    |-- c: integer (nullable = true)
    
// drop field in nested struct
structLevel2.withColumn("a", $"a".withField(
  "a", $"a.a".dropFields("b"))).show
// +--------+
// |       a|
// +--------+
// |[[1, 3]]|
// +--------+
``` 


# Upcoming features

1. Publish to Maven Central.  
2. Add a utility function so that users can manipulate deeply nested fields directly without having to write nested function calls.
3. Add feature to manipulate array/map of struct.   
4. Add spark planning optimization rule to collapse multiple withField/withFieldRenamed/dropFields calls into a single operation.
5. Currently, we have to use one of `$"colName"` or `col("colName")` pattern to access the implicit methods. Should also be able to use `'colName` pattern.
6. Add python bindings. 
