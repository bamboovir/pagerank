# Spark batch pagerank

Set the current directory

```bash
ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}/spark"
```

Install the development environment

[sdkman](https://sdkman.io/install)

```bash
sdk install java 17.0.1
sdk install scala 2.13.10
sdk install sbt 1.7.3
```

Install & Configure Spark

```bash
curl -fsSLO "https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz"
tar xvf spark-3.3.1-bin-hadoop3-scala2.13.tgz
rm spark-3.3.1-bin-hadoop3-scala2.13.tgz
mv spark-3.3.1-bin-hadoop3-scala2.13 /usr/local/spark
export PATH="/usr/local/spark/bin/:$PATH"
```

Verify that Spark has been successfully configured

```bash
# https://spark.apache.org/docs/latest/submitting-applications.html
spark-submit \
  --master "local[4]" \
  --deploy-mode client \
  --class org.apache.spark.examples.SparkPi \
  /usr/local/spark/examples/jars/spark-examples_2.13-3.3.1.jar \
  1000
```

Packaged PageRank Spark App Jar

```bash
# > target/scala-2.13/pagerank-assembly-0.1.0.jar
sbt assembly
```

Run PageRank on Spark locally with K = 4 worker threads

```bash
# Usage: NaivePageRank <verticesFilePath> <edgesFilePath> <tolerance=1e-8> <dampingFactor=0.85> <topK=10>
spark-submit \
  --master "local[4]" \
  --deploy-mode "client" \
  --class "PageRank" \
  "${ROOT}/spark/pagerank/target/scala-2.13/pagerank-assembly-0.1.0.jar" \
  "${ROOT}/dataset/batch/2002-verts.txt" \
  "${ROOT}/dataset/batch/2002-edges.txt" \
  1e-10 \
  0.85 \
  10
```

```bash
# Usage: GraphXPageRank <verticesFilePath> <edgesFilePath> <tolerance=1e-8> <dampingFactor=0.85> <topK=10>
# This solution aims to verify the correctness of our custom implemented PageRank algorithm.
spark-submit \
  --master "local[4]" \
  --deploy-mode "client" \
  --class "GraphXPageRank" \
  "${ROOT}/spark/pagerank/target/scala-2.13/pagerank-assembly-0.1.0.jar" \
  "${ROOT}/dataset/batch/2002-verts.txt" \
  "${ROOT}/dataset/batch/2002-edges.txt" \
  1e-10 \
  0.85 \
  10
```

If you want to use intellij idea to run/debug locally, please configure the following options.

```bash
# Add VM Options
# https://stackoverflow.com/questions/73465937/apache-spark-3-3-0-breaks-on-java-17-with-cannot-access-class-sun-nio-ch-direct
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
-Dspark.master=local[4]
# Add dependencies with "provided" scope to classpath
```
