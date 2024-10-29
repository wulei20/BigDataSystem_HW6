import sys
from pyspark import SparkConf, SparkContext
import time

def parse_neighbors(line):
    src, dst = line.split()
    return int(src), int(dst)

def compute_contribs(nodes, rank):
    num_nodes = len(nodes)
    for node in nodes:
        yield (node, rank / num_nodes)

if __name__ == "__main__":
    
    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setCheckpointDir('checkpoint')
    lines = sc.textFile(sys.argv[1])

    # Start timing
    first = time.time()
    
    # Parse edges and remove duplicates
    links = lines.map(parse_neighbors).distinct().groupByKey().cache()
    
    # Initialize each node's rank to 1.0
    ranks = links.mapValues(lambda _: 1.0)

    # Set damping factor and number of iterations
    d = 0.8
    num_iterations = 25

    # Run PageRank iterations
    for i in range(num_iterations):
        # Calculate each node's contribution to the rank of its neighbors
        contribs = links.join(ranks).flatMap(
            lambda node_neighbors_rank: compute_contribs(node_neighbors_rank[1][0], node_neighbors_rank[1][1])
        )
        
        # Recalculate ranks based on damping factor
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: (1 - d) + d * rank)
    ranks.checkpoint()
    for i in range(num_iterations):
        # Calculate each node's contribution to the rank of its neighbors
        contribs = links.join(ranks).flatMap(
            lambda node_neighbors_rank: compute_contribs(node_neighbors_rank[1][0], node_neighbors_rank[1][1])
        )
        
        # Recalculate ranks based on damping factor
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: (1 - d) + d * rank)

    # Collect top 5 nodes by rank
    top_5 = ranks.takeOrdered(5, key=lambda x: -x[1])

    # Output top 5 nodes and program runtime
    print("Top 5 nodes by PageRank:")
    for node, rank in top_5:
        print(f"Node ID: {node}, PageRank: {rank / ranks.count():.7f}")

    # End timing
    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))

    # Stop Spark context
    sc.stop()
