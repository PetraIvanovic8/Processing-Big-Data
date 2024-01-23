Proposal: [Big Data Project](https://docs.google.com/document/d/1w3zPQwyXO4kyajLx2g-alETq7KZbePB0s7fyvpeRUeo/edit?usp=sharing)

Slides: [Movie Ratings and Revenue Analysis](https://docs.google.com/presentation/d/1XqphU7l2aT25uZczdglWua7tq3-X0T6Ww-z-ZavtNEY/edit?usp=sharing)

Path to Personal HDFS Directory: hdfs://nyu-dataproc-m/user/YOURNETID_nyu_edu/
Contributors: Nabiya Alam (na2794@nyu.edu), Petra Ivanovic (pi2018@nyu.edu), Jon Dinh (jhd9252@nyu.edu)

## IMDb Movie Data Instructions:

1. Go to the following link, https://datasets.imdbws.com/,  and download the these files: name.basics.tsv, title.basics.tsv, title.crew.tsv, title.principals.tsv, title.ratings.tsv, you can consult the documentation of the files here: https://developer.imdb.com/non-commercial-datasets/
2. Upload these files onto dataproc and place them into HDFS using following lines
    1. hdfs dfs -put name.basics.tsv
    2. hdfs dfs -put title.basics.tsv 
    3. hdfs dfs -put title.crew.tsv
    4. hdfs dfs -put title.principals.tsv
    5. hdfs dfs -put title.ratings.tsv

3. Similarly, upload all of the following files into dataproc and place them into HDFS:
    1. hdfs dfs -put imdb_ingest.scala
    2. hdfs dfs -put cleaning_IMDB.scala
    3. hdfs dfs -put imdb_profiling.scala
    4. hdfs dfs -put join_data.scala
    5. hdfs dfs -put joint_data_analysis.scala

4. Open the spark Scala shell: 
    1. `spark-shell --deploy-mode client`

5. To begin - run imdb_ingest.scala as follows to load all of the data:
    1. `:load imdb_ingest.scala`

6. Next - run cleaning_IMDB.scala as follows to merge and clean the data:
    1.  `:load cleaning_IMDB.scala`
    2. This code outputs merged and preprocessed IMDb dataset

7. Next - run imdb_profiling.scala as follows to get to know the dataset better:
    1.  `:load imdb_profiling.scala`
    2. This code outputs additionally processed dataset and some of its statistics

8.  Next - run join_data.scala as follows to merge all 3 of our datasets - IMDb, Small Lense, and Box Office data and do preliminary cleaning of it:
    1.  `:load join_data.scala`
    2. This code outputs the joined dataset of all mentioned datasets in this file with only relevant features for our further analysis, you can also find this data in the directory "hdfs:///user/pi2018_nyu_edu/moviesFinalData_Cleaned/"

9. Next - run joint_data_analysis.scala to do analytics about lifetime gross, release year, average rating, and movie studios on the merged data:
    1.  `:load joint_data_analysis.scala`
    2. This code outputs 3 files in the same hdfs directory called monthlyAverages.csv, monthlyResults.csv, topStudios.csv -> these can be found in ana_code / Petra directory as well
    3. To get these files directly from hdfs to local run following commands
        1. `hdfs dfs -getmerge MonthlyResults_Full /home/pi2018_nyu_edu/monthlyResults.csv`
        2. `hdfs dfs -getmerge MonthlyAverages /home/pi2018_nyu_edu/monthlyResults.csv`
        3. `hdfs dfs -getmerge topStudios /home/pi2018_nyu_edu/topStudios.csv`
    4. After that, select “download file” and enter the exact path of the csv - in this case it should just be csv file names.

This concludes our running of code and analytics. Last 3 files, monthlyAverages.csv, monthlyResults.csv, topStudios.csv can be used to reproduce visualizations shown in ana_code using Tableau. Correlation between average IMDB rating, average gross of a movie and month released can be found using the correlation_calculation.ipynb located in ana_code / Petra

## Small Lens Movie Data:

1. Go to the following link; https://files.grouplens.org/datasets/movielens/ml-latest-small.zip and down the zip files containing;
    1. `Links.csv`
    2. `Movies.csv`
    3. `Ratings.csv`
    4. `Tags.csv`
2. Run the api_script.py (locally before uploading to DataProc, or in DataProc if configured) which will obtain additional data named "api_pull_replace_links.csv"
3. Upload these 5 files to DataProc and then HDFS with the `hdfs dfs -put FileName.ext` command
    1. `hdfs dfs -put links.csv`
    2. `hdfs dfs -put movies.csv`
    3. `hdfs dfs -put ratings.csv`
    4. `hdfs dfs -put tags.csv`
    5. `hdfs dfs -put api_pull_replace_links.csv`
4. Run the `ingest.scala` command using `spark-shell --deploy-mode client -i ingest.scala` 
    1.This combines the data and prepares it for profiling
    2. Input: all original data source files
    3. Output:  output_ingest.csv (Note: Moved and renamed from default DIR output_ingest/part-r-XXXXXXX)
5. Run `profiling.scala" using "spark-shell --deploy-mode client -i profiling.scala` which will provide basic descriptives of the data in the console before any transformations. 
    1. Input: `output_ingest.csv``
    2. Output: None
6. Run `etl_indiv_dinh.scala` using `spark-shell --deploy-mode client -i etl_indiv_dinh.scala`
    1. This file re-combine the original data sources and cleans it for suitable data source joining with teammates, rather than basic descriptives. 
    2. Input: all original data sources files
    3. Output: output_etl_indiv_dinh.csv (Note: Moved and renamed from default DIR from output_etl_indiv_dinh/part-r-XXXXXXXX)
7. Run `etl_comb_dinh.scala` using `spark-shell --deploy-mode client -i etl_indiv_dinh.scala`
    1. This uses the combined dataset from all team members and conducts formatting for analytics. 
    2. Input: `hdfs:///user/pi2018_nyu_edu/moviesFinalData/` If the HDFS access or link is broken, change the input file to `team_uncleaned.csv` if HDFS access is broken.
    3. Output: `output_etl_comb_dinh` (Note: Moved and renamed from default DIR from output_etl_comb_dinh/part-r-XXXXXXXX)
8. Run `ana_code_dinh.scala` using `spark-shell --deploy-mode client -i ana_code_dinh.scala`
    1. This file label encodes the features using StringIndexer from SparkML, converts the data into RDD[VECTOR] format, and obtains analytics. Further adjustments for readability. 
    2. Input: `output_etl_comb_dinh.csv`
    3. Output: unformatted.csv, formatted.csv (Note: Moved and renamed from respective default DIRs). 
9. Run `ana_vis_code.py` or `ana_vis_code.ipynb` for visualizations, locally or on DataProc if configured to obtain visualizations. 
    1. This file uses the same resulting analytics from `ana_code_dinh.scala` (checked) for visualizations, graphs and analytics in a read-able presentation.
    2. Note: Possible to incorporate HDFS in to NB, also spylon-kernal for Scala (not native).
    3. Input: `pre_process.csv`, `process.csv`
    4. Output: None
Note: All code files in their respective directories, also have their output files moved and renamed to those same directories for ease-of-use in HDFS. 



## Box Office Data

Go to the following link and download boxoffice.csv: https://data.world/eliasdabbas/boxofficemojo-alltime-domestic-data 

1. Upload it onto dataproc and place it into HDFS: `hdfs dfs -put boxoffice.csv`
Download the following folder from the submission and upload into DataProc :
Under etl_code/nabiya `MRCleaningIndividualData` It would probably be helpful to rename the folder back to  MRCleaningIndividualData before uploading it if there is a name change.
2. Unzip the folder using the unzip MRCleaningIndividualData Run the map reduce code using the following commands to clean data and get the clean dataset:
    1. `cd MRCleaningIndividualData/demo/target`
    2. `hadoop jar Clean-1.0.jar com.example.Clean ./boxoffice.csv ./cleanboxoffice` The output from this run is located in `hadoop fs -cat cleanboxoffice/part-r-00000`
3. Add the file to your home directory using `hadoop fs -cat cleanboxoffice/part-r-00000 > /home/YOURNETID_nyu_edu/cleanboxoffice.csv`
4. Next, download the following files:
    1. profiling1.scala
    2. profiling2.scala
5. Run the following commands to get the profiling (mean, median, mode, etc.) information about the box office data:
    1. `spark-shell --deploy-mode client -i profiling1.scala`
    2. `spark-shell --deploy-mode client -i profiling1.scala` The output of these files should simply print out.
6. Under `/home/pi2018_nyu_edu` you should see the file `moviesFinalCombinedData_Clean` you should have access to it
7. Upload the file `combinedAnalysis.scala` (should be under ana_code/nabiya) to your hdfs and run it using the command `spark-shell --deploy-mode client -i combinedAnalysis.scala` which should give you information about actors and actresses along with the average lifetime gross of their movies and their ratings. This information should be printed out.
8. The data could be visualized using Tableau, Excel or Google sheets for comparison between the actors’ and actress’ data