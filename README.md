# Comparison with Baseline Index
We compare the performance of our proposed chunk index with the traditional B+ tree index in terms of disk space consumption and key lookup time. The experimental codes are in this repository.
## 1. B+ tree index
We use the popular persistent B+ tree implementation from [this](https://github.com/andylamp/BPlusTree).

## 2. Chunk index with step regression
Our proposed chunk index as described in paper.

## 3. How to experiment

1.   run `mvn clean package` to build jars.
2.   Then go to `target` directory, and you will find `BPlusTreeBaselineExp-jar-with-dependencies.jar`.
3.   Use `java -jar BPlusTreeBaselineExp-jar-with-dependencies.jar Param1 Param2 Param3` to perform the experiments.
     -   Param1: true to test B+ tree, false to test chunk index with step regression
     -   Param2: number of keys
     -   Param3:  test data file path

For example, run the following command:

```
java -jar BPlusTreeBaselineExp-jar-with-dependencies.jar true 1000 D:\full-game\BallSpeed.csv
```

and the experimental results on B+ tree with 1000 keys from BallSpeed dataset are printed in the console window:

>   [Experimental Settings]
>   B+ tree index on 1000 keys
>   data source file: D:\full-game\BallSpeed.csv
>
>   [Experimental Results]
>   file size: 82944, build cost: 2099924810, read index cost: 2361770, search cost: 2402560, read data cost: 217660, total query cost: 4981990

Similarly, run the following command:

```
java -jar BPlusTreeBaselineExp-jar-with-dependencies.jar false 1000 D:\full-game\BallSpeed.csv
```

and the experimental results on our proposed chunk index with 1000 keys from BallSpeed dataset are printed in the console window:

>   [Experimental Settings]
>   Chunk index with step regression
>   on 1000 keys
>   data source file is D:\full-game\BallSpeed.csv
>
>   [Experimental Results]
>   file size: 2224, build cost: 4771240, read index cost: 779470, search cost: 19670, read data cost: 390690, total query cost: 1189830

