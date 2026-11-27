# A guided tour of the Spark UI
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- course setup -------------------------------------------------------------------------------
root = Path.cwd()
while not (root / "1_generate_data.py").exists() and root != root.parent:
    root = root.parent                                 # walk up to the project folder
DATA = str(root / "data")
spark = (SparkSession.builder.master("local[*]").appName("spark_ui_tour")
         .config("spark.sql.session.timeZone", "UTC")             # keep timestamps in UTC
         .config("spark.ui.showConsoleProgress", "false")          # no progress bars in the console
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")                # hide INFO/WARN chatter
# ------------------------------------------------------------------------------------------------

sc = spark.sparkContext
playback = spark.read.parquet(f"{DATA}/raw/playback")
viewers = spark.read.option("header", True).option("inferSchema", True).csv(f"{DATA}/raw/viewers.csv")

sc.setJobDescription("1. count all sessions")
playback.count()
sc.setJobDescription("2. sessions per city (join + shuffle)")
playback.join(viewers, "viewer_id").groupBy("city").count().collect()
sc.setJobDescription("3. top viewers by watch time (sort)")
playback.groupBy("viewer_id").agg(F.sum("watch_seconds").alias("s")).orderBy(F.desc("s")).limit(5).collect()
sc.setJobDescription(None)

jobs = sc.statusTracker().getJobIdsForGroup()
print(f"{len(jobs)} Spark jobs ran. Open {sc.uiWebUrl} in your browser and look at:")
print("  Jobs tab        - one row per action, with the description set above")
print("  Stages tab      - each job split into stages at shuffle boundaries")
print("  SQL tab         - the query plan as a graph, with rows and time per operator")
print("  Executors tab   - the single local executor (your driver), its cores and memory")
print("  Environment tab - the Java/Python versions and every Spark setting in use")
input("The UI lives only while this script runs. Press Enter to stop Spark... ")