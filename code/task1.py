from pyspark import SparkContext
import sys
import os
import time
from itertools import combinations
from graphframes import GraphFrame
from pyspark.sql import SparkSession

start = time.time()

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.8.2-spark3.1-s_2.12 pyspark-shell")

# os.environ["PYSPARK_SUBMIT_ARGS"] = (
#     "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell")

def write_csv_file(communities_rdd, output_file):
    with open(output_file, "w") as f:
        for community in communites_rdd.collect():
            f.write(str(community)[1:-1] + "\n")

if __name__ == "__main__":

    sc = SparkContext()
    SparkSession = SparkSession(sc)
    sc.setLogLevel("ERROR")

    # filter_threshold = int(sys.argv[1])
    # input_file = sys.argv[2]
    # community_output_file = sys.argv[3]

    filter_threshold = 5
    input_file = "data/ub_sample_data.csv"
    community_output_file = "result/task1.txt"

    # Variables
    MAX_ITER = 5

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

    edges_df = sc.parallelize(edges).toDF(["src", "dst"])
    vertices_df = sc.parallelize(vertices).map(lambda uid: (uid,)).toDF(["id"])

    graph = GraphFrame(vertices_df, edges_df)
    communites = graph.labelPropagation(MAX_ITER)

    communites_rdd = communites.rdd.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: sorted(list(x[1]))) \
        .sortBy(lambda community: (len(community), community))

    write_csv_file(communites_rdd, community_output_file)

    print(time.time() - start)
    