# Parallelized Implementation of Page Rank and TF-IDF Algorithms with Apache Spark

**Apache Spark** is a great tool for distributed programming which helps segment a task among multiple cores, and the cores do their sub-tasks in parallel. In this project, I used Apache Spark with **Java** to implement two algorithms: **PageRank** and **Term Frequency-Inverse Document Frequency (TF-IDF)**. In the following, I explain the algorithms and how I parallelized their computations.


## 1. **PageRank**: 

PageRank is an algorithm for finding important nodes inside a graph by assigning a rank to each node. The rank of each node is updated based on an interpolation of its previous rank and its neighbors' ranks, using the formula:

$$PR^t(A) = 0.15 PR^{t-1}(A) + 0.85 \Sigma_{v \in N(A)} \frac{PR(v)}{L(v)}$$

Where $t$ is the current step (the algorithm is iterative and has $T$ steps), $N(A)$ is the set of $A$'s neighboring nodes, and $L(v)$ is the number of outgoing edges from node $v$.

#### Parallelization Approach

In each iteration, each node calculates its own score, $\frac{PR(v)}{L(v)}$, sends it to its neighbors, and then calculate its own updated rank using the above formula. Therefore, the whole nodes can be stored in a **JavaPairRDD<nodeID, <v, vRank>>**, and their load is distributed across multiple cores.

This parallelization remarkably reduces runtime, compared to serial case (single-core), particularly for large graphs. For example, I reached a 40% speedup in a graph of 50,000 nodes, and %18 in a graph of 20,000 nodes. 

## 2. **TF-IDF**: 

TF-IDF is a traditional algorithm in Natural Language Processing (NLP) for text classification. It receives multiple text files as input and finds the most relevant and distinctive words for each document. As its name suggests, it has two components: Term Frequency (TF) and Inverse Document Frequency (IDF).

For an input of $N$ text files, the algorithm works based on the following formulas: 

$$TF(word w, text t) = \frac{number of occurrences of w in t}{size(t)}$$

$$IDF(word w) = log(\frac{N}{number of documents containing w})$$

$$TF-IDF(word w, text t) = TF(w, t) * IDF(w)$$

TF-IDF value is between 0 and $log(N)$, and the higher it gets, the more relevant $w$ is to document $t$.

#### Parallelization Approach

Each file can independently calculate its words' TF scores, so I used a **JavaPairRDD<w, <fileID, TF-Score>>** to handle TF scores parallelization. For IDF scores, I did a **groupByKey** on TF-step output, where key is the word $w$, to obtain number of files having $w$ and then IDF scores. The TF-IDF score is easily calculated afterwards.


