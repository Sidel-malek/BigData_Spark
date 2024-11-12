from pyspark import SparkContext  

# Initialize Spark context
sc = SparkContext(master="spark://spark-master:8080", appName="ArbresApp")  # Corrected Spark Master URL

# 1. Load CSV file into RDD
rdd = sc.textFile("file:///usr/local/spark/data/arbres.csv")

# 1. Count the number of lines in the RDD
num_lines = rdd.count()
print("Number of lines in RDD: {}".format(num_lines))

# Split the data by columns (assuming CSV format)
rdd_split = rdd.map(lambda line: line.split(','))

# 2. Calculate the average height of trees (assuming height is in the 3rd column, index 2)
def safe_float(value):
    try:
        return float(value)
    except ValueError:
        return None  # or handle the error as needed

heights = rdd_split.map(lambda x: safe_float(x[2]))  # Adjust index based on actual data
heights = heights.filter(lambda h: h is not None)  # Filter out None values
avg_height = heights.mean()
print("Average height of trees: {}".format(avg_height))

# 3. Find the genre of the tallest tree (assuming genre is in the 2nd column, index 1)
tree_heights_genre = rdd_split.map(lambda x: (safe_float(x[2]), x[1]))  # (height, genre)
tallest_tree_genre = tree_heights_genre.filter(lambda x: x[0] is not None).sortByKey(ascending=False).first()[1]
print("Genre of the tallest tree: {}".format(tallest_tree_genre))  # Corrected output

# 4. Count the number of trees of each genre
genre_counts = rdd_split.map(lambda x: (x[1], 1))  # (genre, 1)
genre_totals = genre_counts.reduceByKey(lambda a, b: a + b)

# Print number of trees by genre
print("Number of trees by genre:")
for genre, count in genre_totals.collect():
    print("{}: {}".format(genre, count))
