# MapReduce - Cooccurrence

Big Data Assignment

Compare product_title && star_rating by using pairs and stripes 

Lukia - Stripe 
Oliver - Pairs  

1. export output data into .csv
 
2. compare with and without optimisation approaches.

Optimisation:
- combiner
- partitioner
- local aggregation

** Important Reference **
http://codingjunkie.net/cooccurrence/

** How to run **
hadoop jar /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar WordCooc pair s3a://commoncrawl/crawl-data/CC-MAIN-2018-17/segments/1524125936833.6/wet/CC-MAIN-20180419091546-20180419111546-00000.warc.wet.gz /user/jingyi/output1.txt /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar

hadoop jar /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar WordCooc stripe s3a://commoncrawl/crawl-data/CC-MAIN-2018-17/segments/1524125936833.6/wet/CC-MAIN-20180419091546-20180419111546-00000.warc.wet.gz /user/jingyi/output2.txt /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar

格式：
Hadoop     jar     /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar WordCooc    [pair | stripe]     input/path/     /output/path     /home/hadoop/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar

**input path can be + /tmp/*.warc.wet.gz**
