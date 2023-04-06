from pyspark import SparkContext
import sys
import os
import time
from itertools import combinations
from collections import defaultdict, deque

start = time.time()

def write_csv_file(output_file):
    return

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


def calculate_betweeness(graph):
    vertices = list(graph.keys())
    for vertex in vertices:
        visited = set()
        distances = dict()
        shortest_paths = {
            vertex: 1
        }
        levels = {
            vertex: 0
        }

        tree = {vertex: []}

        queue = [(vertex, 0)]

        while queue:
            start = queue.pop(0)
            curr_node = start[0]
            curr_path = start[1]

            visited.add(curr_node)

            tree[curr_node] = list()

            if curr_node not in graph:
                break

            neighbors = graph[curr_node]

            for neighbor in neighbors:
                if neighbor not in visited:
                    if neighbor not in distances:
                        shortest_paths[neighbor] = 1
                        distances[neighbor] = curr_path + 1

                    elif distances[neighbor] == curr_path + 1:
                        shortest_paths[neighbor] += 1

                    if neighbor not in levels:
                        tree[curr_node].append(neighbor)
                        levels[neighbor] = curr_path + 1
                        queue.append((neighbor, curr_path + 1))

                    elif levels[neighbor] > curr_path:
                        tree[curr_node].append(neighbor)
        
        print(vertex, " : ", tree)

        # #STEP 1: Build a Tree
        # tree = build_tree(graph, vertex)

        # #STEP 2: Find the shortest paths
        # shortest_paths = bfs_shortest_paths(graph, vertex)

        # print(vertex, " : ", tree)

    return

# def calculate_betweenness_new(g):  
#     v_list = list(g.keys())
#     bet = defaultdict(float)
#     for root in v_list:
#         parent = defaultdict(set)
#         lvl = {}
#         n_sp = defaultdict(float)
#         path = []
#         queue = []
#         queue.append(root)
#         vis = set()
#         vis.add(root)
#         lvl[root] = 0
#         n_sp[root] = 1
        
#         while len(queue) > 0:
#             root = queue.pop(0)
#             path.append(root)
#             for p in g[root]:
#                 if p not in vis:
#                     queue.append(p)
#                     vis.add(p)
#                     if p not in parent.keys():
#                         parent[p] = set()
#                         parent[p].add(root)
#                     else:
#                         parent[p].add(root)
#                     n_sp[p] += n_sp[root]
#                     lvl[p] = lvl[root] + 1
#                 elif lvl[p] == lvl[root] + 1:
#                     if p not in parent.keys():
#                         parent[p] = set()
#                         parent[p].add(root)
#                     else:
#                         parent[p].add(root)
#                     n_sp[p] += n_sp[root]
#         v_w = {}
#         for p in path:
#             v_w[p] = 1
#         edge_w = defaultdict(float)
#         for p in reversed(path):
#             for q in parent[p]:
#                 temp_w = v_w[p] * (n_sp[q] / n_sp[p])
#                 v_w[q] += temp_w
#                 if tuple(sorted([p, q])) not in edge_w.keys():
#                     edge_w[tuple(sorted([p, q]))] = temp_w
#                 else:
#                     edge_w[tuple(sorted([p, q]))] += temp_w
#         for key, value in edge_w.items():
#             if key not in bet.keys():
#                 bet[key] = value / 2
#             else:
#                 bet[key] += value / 2
#     bet = sorted(bet.items(), key=lambda x: (-x[1], x[0]))
#     #list
#     return bet

# def calculate_betweenness(edges, graph):

#     #STEP 1: Initialize the betweeness centrality of all edges to 0 
#     betweenness = dict()
#     for edge in edges:
#         betweenness[edge] = 0
    
#     #calculate betweenness for each vertex

#     # STEP 2: For each vertex in the graph, find all shortest paths from the vertex to all other vertices. This can be done using Breadth First Search (BFS).

#     for vertex in graph:

#         #initialization
#         vertex_scores = {v: 0 for v in graph}
#         distances = {v:-1 for v in graph}
#         paths = {v:[] for v in graph}
#         queue = [vertex]
#         distances[vertex] = 0 
#         paths[vertex] = [[vertex]]

#         # STEP 3: For each pair of vertices (u,v), find the shortest path from u to v.

#         while queue:
#             current_vertex = queue.pop(0)
#             if distances[current_vertex] == -1:
#                 break

    #         neighbors = graph[current_vertex]

    #         for neighbor in neighbors:
    #             if distances[neighbor] == -1:
    #                 distances[neighbor]  = distances[current_vertex] + 1
    #                 queue.append(neighbor)
    #             if distances[neighbor] == distances[current_vertex] + 1:
    #                 vertex_scores[neighbor] += vertex_scores[current_vertex] + 1
    #                 paths[neighbor].extend([path + [neighbor] for path in paths[current_vertex]])
        

    #     # STEP 4: For each edge in the shortest path from u to v, increment its betweenness centrality by 1/num_shortest_paths, where num_shortest_paths is the number of shortest paths from u to v that pass through the edge.

    #     for v in paths:
    #         for path in paths[v]:
    #             for i in range(len(path) - 1):
    #                 e = (path[i], path[i+1])
    #                 betweenness[e] += vertex_scores[v]/len(paths[v])

    # # STEP 5: Repeat steps 2-4 for all vertices in the graph.

    # # STEP 6: Divide the betweenness centrality of each edge by 2 to account for the fact that each shortest path is counted twice (once in each direction).
    # for edge in betweenness:
    #     betweenness[edge] /= 2

    # # return sorted(betweenness.items(), key=lambda x: (-x[1], x[0][0], x[0][1]))
    # # STEP 7: Sort the edges in descending order of betweenness centrality.
    #     # 
   
    # # for i in sorted(betweenness):
    # #     print((i, betweenness[i]), end = "\n")

    # for k,v in betweenness.items():
    #     print("Edges: ", k, " - Betweeness: ", v, "\n")


def build_graph(vertices, edges):
    graph = dict()
    for vertex in vertices:
        graph[vertex] = []
    
    for edge1, edge2 in edges:
        graph[edge1].append(edge2)
        graph[edge2].append(edge1)

    return graph

if __name__ == "__main__":

    sc = SparkContext()
    sc.setLogLevel("ERROR")

    # filter_threshold = int(sys.argv[1])
    # input_file = sys.argv[2]
    # community_output_file = sys.argv[3]

    filter_threshold = 5
    input_file = "data/ub_sample_data.csv"
    betweenness_output_file = "result/task2_1.txt"
    community_output_file = "result/task2_2.txt"


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

    vertices = list(vertices)

    # Implement Girvan Newman Algorithm

    # graph = build_graph_cg(vertices, edges)
    
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

    test_betweenness = calculate_betweeness(test_graph)
    for edge, score in test_betweenness:
        print(f"Edge: {edge} - Betweenness Score: {score}")
   
    #TODO: Write this to a txt file

    #TODO: Calculate Betweenness between 2 edges of the graph
    #TODO: Write this to a txt file


    # write_csv_file(community_output_file)

    # print(time.time() - start)
