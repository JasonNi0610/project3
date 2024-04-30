# Large Scale Data Processing: Project 3

## 1. **(4 points)** Implement the `verifyMIS` function. The function accepts a Graph[Int, Int] object as its input. Each vertex of the graph is labeled with 1 or -1, indicating whether or not a vertex is in the MIS. `verifyMIS` should return `true` if the labeled vertices form an MIS and `false` otherwise.

```
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | Yes        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes        |

```


## 2. **(3 points)** Implement the `LubyMIS` function. The function accepts a Graph[Int, Int] object as its input. You can ignore the two integers associated with the vertex RDD and the edge RDD as they are dummy fields. `LubyMIS` should return a Graph[Int, Int] object such that the integer in a vertex's data field denotes whether or not the vertex is in the MIS, with 1 signifying membership and -1 signifying non-membership. The output will be written as a CSV file to the output path you provide. 
```
|        Graph file       | Number of Iterations | Running Time | Is an MIS? |
| ----------------------- | -------------------- | ------------ | ---------- |
| small_edges.csv         | 1                    | 0.131s       | Yes        |
| line_100_edges.csv      | 1                    | 0.137s       | Yes        |
| twitter_100_edges.csv   | 1                    | 0.116s       | Yes        |
| twitter_1000_edges.csv  | 1                    | 0.116s       | Yes        |
| twitter_10000_edges.csv | 1                    | 0.233s       | Yes        |

```
## 3. **(3 points)**  
# a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores. Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`. 

![4011712877527_ pic](https://github.com/JasonNi0610/project3/assets/66149464/a9e31b35-29d3-4576-9c71-31603074267b)
The result was also verified. 


# b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores and then 2x2 cores. Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.

This is the 4*2 cores
![16b654cf2605c6de09a0ee6cbcd75d2c](https://github.com/JasonNi0610/project3/assets/66149464/69f36c8c-313e-428c-94a4-68e589b35893)

This is the 2*2 cores
![f211a113527e345304b39aaf74d37f62](https://github.com/JasonNi0610/project3/assets/66149464/8f4ff804-0bf3-4b82-bee6-4bcc8e01d3a1)

From the results, it seems like the smaller clusters perform competitively than the larger clusters, such as 3*4. Even though more cores means more power for processing, fewer cores might still be faster due to the probable computing inefficiency of more cores.

## Group Members
Qianhui(Kelsey) Zeng, Zihan(Jason) Ni, Peiyu(Ekko) Zhong
