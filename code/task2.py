from pyspark import SparkContext
import sys
import os
import time
from itertools import combinations
from collections import defaultdict, deque

start = time.time()

def write_csv_file(output_file, result):
    with open(output_file, 'w') as f:
        for row in result:
            f.writelines(str(row[0]) + ', ' + str(row[1]) + '\n')
    return

def write_communities(output_file, best_graph):
    with open(output_file, "w") as f:
        for community in best_graph:
            f.write(str(community)[1:-1] + "\n")


def build_tree(graph, root):
    visited = set()
    tree = dict()
    levels = {
        root: 0
    }

    queue = [(root, 0)]

    while queue:
        start = queue.pop(0)
        curr_node = start[0]
        curr_path = start[1]

        tree[curr_node] = list()

        visited.add(curr_node)

        neighbors = graph[curr_node]

        for neighbor in neighbors:
            if neighbor not in visited:
                if neighbor not in levels:
                    tree[curr_node].append(neighbor)
                    levels[neighbor] = curr_path + 1
                    queue.append((neighbor, curr_path + 1))

                elif levels[neighbor] > curr_path:
                    tree[curr_node].append(neighbor)
    return tree

def bfs_shortest_paths(graph, root):
    visited = set()
    distances = dict()
    shortest_paths = {
        root: 1
    }

    queue = [(root, 0)]

    while queue:
        start = queue.pop(0)
        curr_node = start[0]
        curr_path = start[1]

        if curr_node not in graph:
            break

        visited.add(curr_node)

        neighbors = graph[curr_node]

        for neighbor in neighbors:
            if neighbor not in visited:
                if neighbor not in distances:
                    shortest_paths[neighbor] = 1
                    distances[neighbor] = curr_path + 1

                elif distances[neighbor] == curr_path + 1:
                    shortest_paths[neighbor] += 1
                
                queue.append((neighbor, curr_path + 1))

    return shortest_paths


def give_credits(vertex, tree, shortest_paths):
    visited_edges = set()
    node_credit = {vertex: 1}
    edge_credit = defaultdict(float)
    
    def calculate_credits(node):
        if node not in node_credit:
            node_credit[node] = 1
        
        children = tree[node]
        
        for child in children:
            edge = tuple(sorted((node, child)))
            if edge not in visited_edges:
                calculate_credits(child)
                
                child_credit = node_credit[child]
                edge_credit[edge] = (shortest_paths[node] / shortest_paths[child]) * child_credit
                node_credit[node] += edge_credit[edge]
            
            visited_edges.add(edge)
    
    calculate_credits(vertex)
    
    return list(edge_credit.items())

def calculate_betweeness(vertex, graph):
    # vertices = list(graph.keys())
    # betweenness = defaultdict(float)

    # for vertex in vertices:

    tree = build_tree(graph, vertex)
    shortest_paths = bfs_shortest_paths(graph, vertex)
    edge_credits = give_credits(vertex, tree, shortest_paths)
    #     for key, value in edge_credits:
    #         if key not in betweenness.keys():
    #             betweenness[key] = value / 2
    #         else:
    #             betweenness[key] += value / 2

    # bet = sorted(betweenness.items(), key=lambda x: (-x[1], x[0]))
    # #list
    # return bet
    return edge_credits

def calculate_modularity(vertices, betweenness):
    mod = 0
    node_pairs = (list(combinations(vertices, 2)))
    for node_pair in node_pairs:
        node_pair = tuple(sorted(node_pair))
        A = 1 if node_pair in betweenness else 0
        k_i = degree[node_pair[0]]
        k_j = degree[node_pair[1]]

        mod += (A - (k_i * k_j)/(2*M))
    return mod

def find_all_subgraphs(graph):
    subgraphs = list()
    visited = set()
    vertices = list(graph.keys())
    
    for vertex in vertices:
        if vertex not in visited:
            curr_subgraph = list()
            queue = [vertex]     
            while queue:
                curr_node = queue.pop(0)
                if curr_node not in visited:
                    curr_subgraph.append(curr_node)

                    neighbors = graph[curr_node]

                    for neighbor in neighbors:
                        if neighbor not in visited:
                            queue.append(neighbor)

                visited.add(curr_node)
            
            subgraphs.append(curr_subgraph)
    
    return subgraphs


def get_subgraphs(graph):
    visited = set()
    vertices = list(graph.keys()) 
    all_subgraphs = list()
     
    for vertex in vertices:
        if vertex not in visited:
            queue = [vertex]
            curr_subgraph = list()
            while queue:
                curr_node = queue.pop(0)
                if curr_node not in visited:
                    curr_subgraph.append(curr_node)

                    neighbors = graph[curr_node]

                    for neighbor in neighbors:
                        if neighbor not in visited:
                            queue.append(neighbor)
                visited.add(curr_node)

            all_subgraphs.append(curr_subgraph)
    
    return subgraphs

def build_graph(edges):
    graph = defaultdict(set)
    for edge1, edge2 in edges:
        graph[edge1].add(edge2)
    return graph

if __name__ == "__main__":

    sc = SparkContext()
    sc.setLogLevel("ERROR")

    filter_threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    betweenness_output_file = sys.argv[3]
    community_output_file = sys.argv[4]

    # filter_threshold = 7
    # input_file = "data/ub_sample_data.csv"
    # betweenness_output_file = "result/task2_1.txt"
    # community_output_file = "result/task2_2.txt"


    data_RDD = sc.textFile(input_file)
    header = data_RDD.first()
    user_bus_dict = data_RDD.filter(lambda row: row != header)\
                    .map(lambda row: row.split(","))\
                    .map(lambda row: (row[0], row[1]))\
                    .groupByKey()\
                    .mapValues(lambda bid: set(sorted(bid)))\
                    .collectAsMap()

    user_pair_list = list(combinations(list(user_bus_dict.keys()), 2))

    edges = list()
    vertices = set()

    for user1, user2 in user_pair_list:
        if len(set(user_bus_dict[user1]) & set(user_bus_dict[user2])) >= filter_threshold:
            edges.append((user1, user2))
            edges.append((user2, user1))
            vertices.add(user1)
            vertices.add(user2)

    # Implement Girvan Newman Algorithm

    graph = build_graph(edges)
    
    test_graph = {
        'A': ['B', 'C', 'D'], 
        'B': ['A', 'C'],
        'C': ['B', 'A', 'D'],
        'D': ['A', 'C', 'E'],
        'E': ['D', 'F', 'G', 'H'],
        'F': ['E', 'G'],
        'G': ['F', 'E', 'H'],
        'H': ['E', 'G']
    }

    test_edges = {
        ('A', 'B'),
        ('A', 'C'),
        ('A', 'D'),
        ('B', 'C'),
        ('C', 'D'),
        ('D', 'E'),
        ('E', 'F'),
        ('E', 'G'),
        ('E', 'H'),
        ('F', 'G'),
        ('G', 'H'),
        ('B', 'A'),
        ('C', 'A'),
        ('D', 'A'),
        ('C', 'B'),
        ('D', 'C'),
        ('E', 'D'),
        ('F', 'E'),
        ('G', 'E'),
        ('H', 'E'),
        ('G', 'F'),
        ('H', 'G'),
    }

    #TODO: Calculate Betweenness between 2 edges of the graph

    vertices_rdd = sc.parallelize(list(vertices))

    betweenness_agg_rdd = vertices_rdd.flatMap(lambda node: calculate_betweeness(node, graph)).reduceByKey(lambda x, y: x + y)

    betweenness_output = betweenness_agg_rdd.map(lambda x: (x[0], round(x[1] / 2, 5))).sortBy(lambda x: (-x[1], x[0])).collect()

    #TODO: Write this to a txt file

    write_csv_file(betweenness_output_file, betweenness_output)

    #TODO: Detect Communities

    M = len(betweenness_output)
    degree = {node: len(graph[node]) for node in graph}
    max_modularity = -float('inf')
    #calculate modularity for the original graph

    adjacency_list_RDD = sc.parallelize(list(graph))

    betweenness = adjacency_list_RDD.flatMap(lambda node: calculate_betweeness(node, graph)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], x[1]/2)).collectAsMap()

    modularity = calculate_modularity(vertices, betweenness)

    normalized_modularity = modularity/(2*M)

    if normalized_modularity > max_modularity:
        max_modularity = normalized_modularity

    highest_modularity_graph = [list(vertices)]
    subgraphs = [highest_modularity_graph]

    num_vertices = len(vertices)

    while len(subgraphs) != num_vertices:

        edges_to_remove = [(node1, node2) for (node1, node2), credit in betweenness.items() if credit == max(betweenness.values())]

        for node1, node2 in edges_to_remove:
            graph[node1].remove(node2)
            graph[node2].remove(node1)
        
        betweenness = adjacency_list_RDD.flatMap(lambda node: calculate_betweeness(node, graph)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], x[1]/2)).collectAsMap()

        subgraphs = find_all_subgraphs(graph)

        subgraphs_rdd = sc.parallelize(subgraphs)

        modularity = (subgraphs_rdd.map(lambda x: calculate_modularity(x, betweenness)).reduce(lambda x,y:x+y))/(2*M)

        highest_modularity_graph, max_modularity = (subgraphs, modularity) if modularity > max_modularity else (highest_modularity_graph, max_modularity)

    best_graph = sc.parallelize(highest_modularity_graph).map(lambda x: sorted(x)).sortBy(lambda x: (len(x), x)).collect()

    #TODO: Write this to a txt file
    write_communities(community_output_file, best_graph)


    print(time.time() - start)