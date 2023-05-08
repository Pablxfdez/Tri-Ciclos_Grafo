from pyspark import SparkContext
import sys

'''
PABLO FERNÁNDEZ DEL AMO
04231435X
'''

def edges(line):
# Splitting line by comma to obtain two nodes
    nodes = line.strip().split(',')

    if nodes[0] < nodes[1]:
    # Returning nodes in ascending order
        return (nodes[0], nodes[1])
    elif nodes[1] > nodes[0]:
        return (nodes[1], nodes[0])
    else:
    # If nodes are equal, return None
        pass
    
    
def lista_adyacentes(node_list):
    node = node_list[0]
    rest = node_list[1]
    adjacent_list = []
    for i in range(len(rest)):
        n = rest[i]
        # Adding node to adjacent list with status "exists"
        adjacent_list.append(((node, n), 'exists'))
        for j in range(i+1, len(rest)):
            if rest[i] < rest[j]:
                # Adding edge to adjacent list with status "pending" and node names in ascending order
                adjacent_list.append(((rest[i], rest[j]), ('pending', node)))
            else:
                # Adding edge to adjacent list with status "pending" and node names in ascending order
                adjacent_list.append(((rest[j], rest[i]), ('pending', node)))
    return adjacent_list

# Function that receives a list of tricycles and returns a list of tuples
def get_final_tricycles(tricycles):
    final_tricycles = []

    # Iterate over the pairs and their messages
    for edge, messages in tricycles.collect():
        message_list = list(messages)
        # Check if there are more than one message and if the word 'exists' is in the messages
        if len(message_list) > 1 and 'exists' in message_list:
            # Iterate over the messages and add them to the final list if they are not 'exists'
            for m in message_list:
                if m != 'exists': 
                    final_tricycles.append((m[1], edge[0], edge[1]))
    
    return sorted(final_tricycles)


def main(sc, filename_list):
    for filename in filename_list:
        # Read the file into an RDD
        graph = sc.textFile(filename)
        
        # Create the graph by mapping the edges, removing duplicates, and filtering out None values
        graph_edges = graph.map(edges).distinct().filter(lambda x: x is not None)
        
        # Group the adjacent nodes and sort by key
        adjacent_nodes = graph_edges.groupByKey().map(lambda x: (x[0], sorted(list(x[1])))).sortByKey()
        
        # Flatmap the adjacent nodes and group by key to get the tricycles
        tricycles = adjacent_nodes.flatMap(lista_adyacentes).groupByKey()
        
        print(f' The tricycles in {filename} are: \n-> {get_final_tricycles(tricycles)}')


if __name__=="__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 {0} <list of files>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            main(sc, sys.argv[1:])