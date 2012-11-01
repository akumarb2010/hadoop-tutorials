-- sed.pig
-- pig-training.jar contains custom udfs developed by a user. It has to be registered
-- before running the pig script.
REGISTER pig-training.jar;
A = LOAD '/input/sample.txt' AS (line: chararray);
B = FOREACH A GENERATE tutorial.hadoop.pig.training.udf.Transform(line);
STORE B INTO '/output/sed';
