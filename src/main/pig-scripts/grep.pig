lines = LOAD '/input/pig/sample.txt';
hadoopLines = FILTER lines BY $0 MATCHES '.*hadoop.*';
STORE hadoopLines INTO '/output/pig/cleanedLines1';

