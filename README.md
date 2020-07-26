# AdsRevenue Guide
## How to Run
### parameters 
1) -i or --impressions-file-path absolute path of impressions.json
2) -c or --clicks-file-path absolute path of clicks.json
3) -o or --output-file-path optional. the absolute path of the outputfile
4) -n or --topN optional, default 5. the number of top advertisers you want the application to find.  
 
to run is sbt-shell. goto project path and or can also run from sbt-shell 
```sbtshell
sbt run -i absolute_path_of_impressions.json -c absolute_path_of_clicks.json
```

for tests
```sbtshell
sbt test
```

to generate jar
```sbtshell
assembly
```
 
to run the application
```bash
./spark-submit --master local[*] asbolute_path_to_AdvertiserRecommender-assembly-0.1-SNAPSHOT.jar -i impressions.json -c clicks.json
```
It assumes that the Spark Environment is present where the jar is being run else change spark dependencies to Compiled from Provided

# TBD
Integration tests
