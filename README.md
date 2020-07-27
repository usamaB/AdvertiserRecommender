# AdvertiserRecommender Guide
## Decision 
Remove impressions with null or country_code set as Empty. from empty the analyst or Data Scientist can't tell which country it is. 
If want to enable it's just a check.
I take only the top 5 advertisers even if some have the same rate. If you want top 5 with different rates change
[line](https://github.com/usamaB/AdvertiserRecommender/blob/master/src/main/scala/org/usama/aidrecommender/jobs/AdvertiserRecommender.scala#L162) from row_number() to dense_rank()

## How to Run
### parameters 
1) -i or --impressions-file-path absolute path of impressions.json
2) -c or --clicks-file-path absolute path of clicks.json
3) -o or --output-file-path optional. the absolute path of the outputfile. Default is recommended_advertisers.json
4) -n or --topN optional, default 5. the number of top advertisers you want the application to find.  
 
to run is sbt-shell. goto project path and or can also run from sbt-shell. Would have to change spark dependencies from provided to compiled
```sbtshell
sbt run -i absolute_path_of_impressions.json -c absolute_path_of_clicks.json
```

for tests
```sbtshell
sbt test
```

to generate jar run in sbt-shell
```sbtshell
assembly
```
 
to run the application
```bash 
./spark-submit --master local[*] --class "org.usama.aidrecommender.Main" asbolute_path_to_AdvertiserRecommender.jar -i impressions.json -c clicks.json
```
It assumes that the Spark Environment is present where the jar is being run else change spark dependencies to Compiled from Provided

## Result
If you provide -o or --output-file-path parameter the output will be generated at that path. else it'll be generated at the same path you run the command/application.
Two output files are generated one with single line, second with multi-line.
Single-line-format: ```{"app_id":32,"country_code":"US","recommended_advertiser_ids":[4,9,5,1,3]}```
Multi-line-format:  ```
{
  "app_id" : 32,
  "country_code" : "US",
  "recommended_advertiser_ids" : [
    4,
    9,
    5,
    1,
    3
  ]
}
```

# TBD
Integration tests
