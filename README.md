# Twitter ETL Pipeline

#### This pipeline ingests and transforms data from the Twitter API. Specifically, data is ingested from the @FIFAWorldCup Twitter Page. Data is cleansed, transformed, and inserted into an RDS Database.

Airflow is used to schedule these data transformations and uploads. DAGs run on 6-hour intervals and update the database with any new tweets that were made from the FIFAWorldCup Twitter page.

Opportunities for Growth:
This database counts the number of retweets, replies, likes, and quotes at the time of the API call. When the next request is made, only tweets that don't match the existing tweets in the database are added. An area for optimization would be to come up with a way to update the existing tweets with their new engagement metric counts, along with the time for which they have been posted for (more recent tweets generally have lower engagement than those that have been posted for longer periods of time).

![ezgif com-gif-maker](https://user-images.githubusercontent.com/75954323/207465422-5f9aa80c-5c6b-43fe-8ff8-fad11ce0c386.jpg)
