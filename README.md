# Tri-Ciclos en un Grafo

1. Tricycles, or more formally 3-cycles, are a basic element in the analysis of graphs and subgraphs. 3-cycles show a close relationship between 3 entities (vertices): each vertex is related (has edges) to the other two.
Write a parallel program that calculates the 3-cycles of a graph defined as a list of edges. 

-- This problem is solved in #Tricycles1.py

2. Assume that the data, i.e., the list of edges, is not in a single file but in many files.
Write a parallel program that calculates the 3-cycles of a graph that is defined in multiple input files.

-- This problem is solved in #Tricycles2.py

3. Suppose that the data of the graph is distributed in multiple files. We want to calculate the 3-cycles, but only those that are local to each file. Write a parallel program that independently calculates the 3-cycles of each input file.

-- This problem is solved in #Tricycles3.py
