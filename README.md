# Make Structs Easy (MSE)

This library adds `withField`, `withFieldRenamed`, and `dropFields` (implicit) methods to the Column class to enable easy StructType column manipulation. 
The signature and behaviour of these methods is meant to be similar to their Dataset equivalents, namely the `withColumn`, `withColumnRenamed`, and `drop` methods.
The methods themselves have been implemented using Catalyst Expressions and so should provide good performance (and certainly better than UDFs). 

# Supported Spark versions
MSE should work without any further requirements on Spark 2.4.x. 

# Installation

## SBT Project

To include this library in an SBT project, you can use [sbt-github-packages plugin](https://github.com/djspiewak/sbt-github-packages). 
See the link for up-to-date instructions but basically, you need to:  

1. Include the plugin in `project/plugins.sbt`: 
```scala
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "0.2.1")
```

2. Include this library as a dependency in `build.sbt`: 
```scala
libraryDependencies ++= Seq(
  "mse" %% "mse" % "0.1.1"
)

resolvers += Resolver.githubPackagesRepo("fqaiser94", "mse")

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  // THIS IS NOT SECURE, BE CAREFUL!
  "GITHUB_USERNAME", 
  "GITHUB_ACCESS_TOKEN")
```

## Other Projects (e.g. Maven, Gradle)

See [GitHub docs](https://help.github.com/en/github/managing-packages-with-github-packages/using-github-packages-with-your-projects-ecosystem) for details. 

## spark-shell or spark-submit

Download the latest jar from [GitHub packages](https://github.com/fqaiser94/mse/packages). Make sure you choose the correct Scala version for your needs.
Start a spark-shell session and include the path to downloaded jar:    

```bash
spark-shell --jars mse_2.11-0.1.jar
``` 

# Usage 

To bring in to scope the (implicit) Column methods, use:   

```scala
import com.mse.column.methods._
```

You can now use these methods to manipulate fields in a top-level StructType column: 

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

You can also use these methods to manipulate fields in deeply nested StructType columns: 

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

Another common use-case is to perform these operations on array of structs. 
To do this using the Scala APIs, we recommend combining the functions in this library with the functions provided in [spark-hofs](https://github.com/AbsaOSS/spark-hofs/):

```bash
spark-shell --jars mse_2.11-0.1.jar --packages "za.co.absa:spark-hofs_2.11:0.4.0"
```   

```scala
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.mse.column.methods._
import za.co.absa.spark.hofs._

// Generate some example data
val arrayOfStructs = spark.createDataFrame(sc.parallelize(
    Row(List(Row(1, null, 3), Row(4, null, 6))) :: Nil),
    StructType(Seq(
      StructField("array", ArrayType(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType), 
          StructField("c", IntegerType)))))))).cache
          
arrayOfStructs.show
// +------------------+
// |             array|
// +------------------+
// |[[1,, 3], [4,, 6]]|
// +------------------+

arrayOfStructs.printSchema
// root
//  |-- array: array (nullable = true)
//  |    |-- element: struct (containsNull = true)
//  |    |    |-- a: integer (nullable = true)
//  |    |    |-- b: integer (nullable = true)
//  |    |    |-- c: integer (nullable = true)

// add new field to each struct element of array 
arrayOfStructs.withColumn("array", transform($"array", elem => elem.withField("d", lit("hello")))).show(false)
// +--------------------------------+
// |array                           |
// +--------------------------------+
// |[[1,, 3, hello], [4,, 6, hello]]|
// +--------------------------------+

// replace field in each struct element of array
arrayOfStructs.withColumn("array", transform($"array", elem => elem.withField("b", elem.getField("a") + 1))).show(false)
// +----------------------+
// |array                 |
// +----------------------+
// |[[1, 2, 3], [4, 5, 6]]|
// +----------------------+

// rename field in each struct element of array
arrayOfStructs.withColumn("array", transform($"array", elem => elem.withFieldRenamed("b", "z"))).printSchema
// root
//  |-- array: array (nullable = true)
//  |    |-- element: struct (containsNull = true)
//  |    |    |-- a: integer (nullable = true)
//  |    |    |-- z: integer (nullable = true)
//  |    |    |-- c: integer (nullable = true)

// drop field in each Struct element of array
arrayOfStructs.withColumn("array", transform($"array", elem => elem.dropFields("b"))).show(false)
// +----------------+
// |array           |
// +----------------+
// |[[1, 3], [4, 6]]|
// +----------------+
```

# Questions/Thoughts/Concerns?

Feel free to submit an issue. 

# Upcoming features

1. Publish to Maven Central.  
2. Add a utility function so that users can manipulate deeply nested fields directly without having to write nested function calls.
3. Add spark planning optimization rule to collapse multiple withField/withFieldRenamed/dropFields calls into a single operation.
4. Currently, we have to use one of `$"colName"` or `col("colName")` pattern to access the implicit methods. Should also be able to use `'colName` pattern.
5. Add python bindings. 
