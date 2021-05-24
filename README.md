[root@serv02 ~]# spark-shell 
21/05/24 06:42:32 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Spark context Web UI available at ***
Spark context available as 'sc' (master = local[*], app id = local-1621852967843).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.1
      /_/
         
Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 1.8.0_252)
Type in expressions to have them evaluated.
Type :help for more information.

scala> 

scala> 

scala> 

scala> 

scala> 

scala> 

scala> 

scala> val rdd = spark.read.textFile("/tmp/file01.csv")
rdd: org.apache.spark.sql.Dataset[String] = [value: string]

scala> 

scala> 

scala> rdd.collect()
res0: Array[String] = Array(Khadija,Moumni,Marocaine,20, Patrick,Sebastien,Français,59, Moussa,Kebe,Senegalais,30, fdaslkj,lkjfsda,Marocaine,19, kjgfds,dashj,Français,29, klasjh,lfds,Senegalais,10)

scala> rdd.count()
res1: Long = 6

scala> rdd.first()
res2: String = Khadija,Moumni,Marocaine,20

scala> rdd.show(2)
+--------------------+
|               value|
+--------------------+
|Khadija,Moumni,Ma...|
|Patrick,Sebastien...|
+--------------------+
only showing top 2 rows


scala> 

scala> rdd.show(3)
+--------------------+
|               value|
+--------------------+
|Khadija,Moumni,Ma...|
|Patrick,Sebastien...|
|Moussa,Kebe,Seneg...|
+--------------------+
only showing top 3 rows


scala> rdd.take(2)
res5: Array[String] = Array(Khadija,Moumni,Marocaine,20, Patrick,Sebastien,Français,59)

scala> rdd.filter(l => l.contains("Marocaine")).count()
res6: Long = 2

scala> 

scala> 

scala> 

scala> rdd.filter(l => l.split(",")(2).equals("Marocaine")).count()
res7: Long = 2

scala> 

scala> 

scala> rdd.filter(l => l.split(",")(2).equals("Marocaine"))
res8: org.apache.spark.sql.Dataset[String] = [value: string]

scala> val rddMarocaine = rdd.filter(l => l.split(",")(2).equals("Marocaine"))
rddMarocaine: org.apache.spark.sql.Dataset[String] = [value: string]

scala> rddMarocaine.collect()
res9: Array[String] = Array(Khadija,Moumni,Marocaine,20, fdaslkj,lkjfsda,Marocaine,19)

scala> 
