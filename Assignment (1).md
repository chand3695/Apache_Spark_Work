match_details
a row for every players performance in a match


```python
from pyspark.sql import SparkSession

```


```python
spark = SparkSession.builder.appName("PlayerPerformance").getOrCreate()

```

    24/12/08 13:26:21 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.



```python
file_path = "match_details.csv"
```


```python
match_details_df = spark.read.csv(file_path, header=True, inferSchema=True)
```

                                                                                    


```python
match_details_df.printSchema()
```

    root
     |-- match_id: string (nullable = true)
     |-- player_gamertag: string (nullable = true)
     |-- previous_spartan_rank: integer (nullable = true)
     |-- spartan_rank: integer (nullable = true)
     |-- previous_total_xp: integer (nullable = true)
     |-- total_xp: integer (nullable = true)
     |-- previous_csr_tier: integer (nullable = true)
     |-- previous_csr_designation: integer (nullable = true)
     |-- previous_csr: integer (nullable = true)
     |-- previous_csr_percent_to_next_tier: integer (nullable = true)
     |-- previous_csr_rank: integer (nullable = true)
     |-- current_csr_tier: integer (nullable = true)
     |-- current_csr_designation: integer (nullable = true)
     |-- current_csr: integer (nullable = true)
     |-- current_csr_percent_to_next_tier: integer (nullable = true)
     |-- current_csr_rank: integer (nullable = true)
     |-- player_rank_on_team: integer (nullable = true)
     |-- player_finished: boolean (nullable = true)
     |-- player_average_life: string (nullable = true)
     |-- player_total_kills: integer (nullable = true)
     |-- player_total_headshots: integer (nullable = true)
     |-- player_total_weapon_damage: double (nullable = true)
     |-- player_total_shots_landed: integer (nullable = true)
     |-- player_total_melee_kills: integer (nullable = true)
     |-- player_total_melee_damage: double (nullable = true)
     |-- player_total_assassinations: integer (nullable = true)
     |-- player_total_ground_pound_kills: integer (nullable = true)
     |-- player_total_shoulder_bash_kills: integer (nullable = true)
     |-- player_total_grenade_damage: double (nullable = true)
     |-- player_total_power_weapon_damage: double (nullable = true)
     |-- player_total_power_weapon_grabs: integer (nullable = true)
     |-- player_total_deaths: integer (nullable = true)
     |-- player_total_assists: integer (nullable = true)
     |-- player_total_grenade_kills: integer (nullable = true)
     |-- did_win: integer (nullable = true)
     |-- team_id: integer (nullable = true)
    



```python
match_details_df.show(5) 
```

    24/12/08 13:27:21 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.


    +--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+
    |            match_id|player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|
    +--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+
    |71d79b23-4143-435...|      taterbase|                    5|           5|            12537|   13383|                1|                       3|           0|                               98|             NULL|               2|                      3|          0|                              26|            NULL|                  4|          false|        PT14.81149S|                 6|                     4|                     255.0|                       28|                       0|                      0.0|                          0|                              0|                               0|                        0.0|                             0.0|                              0|                 13|                   1|                         0|      1|      1|
    |71d79b23-4143-435...| SuPeRSaYaInG0D|                   18|          18|           131943|  132557|                2|                       3|           0|                                2|             NULL|               1|                      3|          0|                              76|            NULL|                  7|          false|      PT11.2990845S|                 7|                     3|        350.58792304992676|                       49|                       1|                     45.0|                          0|                              0|                               0|                        0.0|                             0.0|                              0|                 18|                   2|                         0|      0|      0|
    |71d79b23-4143-435...|       EcZachly|                   21|          21|           168811|  169762|                2|                       5|           0|                               94|             NULL|               3|                      5|          0|                              24|            NULL|                  3|          false|      PT19.1357063S|                12|                    12|                     625.0|                       43|                       0|                      0.0|                          0|                              0|                               0|                        0.0|                             0.0|                              0|                 10|                   4|                         0|      1|      1|
    |71d79b23-4143-435...|    johnsnake04|                   14|          14|            64073|   64639|             NULL|                    NULL|        NULL|                             NULL|             NULL|            NULL|                   NULL|       NULL|                            NULL|            NULL|                  6|          false|      PT21.1521599S|                13|                    13|                     605.0|                       24|                       0|                      0.0|                          0|                              0|                               0|                        0.0|                             0.0|                              0|                  9|                   2|                         0|      0|      0|
    |71d79b23-4143-435...| Super Mac Bros|                   26|          26|           243425|  244430|                1|                       5|           0|                               86|             NULL|               2|                      5|          0|                               8|            NULL|                  2|          false|      PT12.8373793S|                13|                    12|                     595.0|                       32|                       1|       20.004501342773438|                          0|                              0|                               0|                        0.0|                             0.0|                              0|                 15|                   2|                         0|      1|      1|
    +--------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+
    only showing top 5 rows
    



```python
player_performance_df = match_details_df.select(
    "match_id",
    "player_gamertag",
    "player_total_kills",
    "player_total_headshots",
    "player_total_weapon_damage",
    "player_total_assists",
    "player_total_deaths",
    "player_rank_on_team",
    "team_id"
)

```


```python
player_performance_df.show(truncate=False)

```

    +------------------------------------+---------------+------------------+----------------------+--------------------------+--------------------+-------------------+-------------------+-------+
    |match_id                            |player_gamertag|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_assists|player_total_deaths|player_rank_on_team|team_id|
    +------------------------------------+---------------+------------------+----------------------+--------------------------+--------------------+-------------------+-------------------+-------+
    |71d79b23-4143-4359-a62e-489a27597b23|taterbase      |6                 |4                     |255.0                     |1                   |13                 |4                  |1      |
    |71d79b23-4143-4359-a62e-489a27597b23|SuPeRSaYaInG0D |7                 |3                     |350.58792304992676        |2                   |18                 |7                  |0      |
    |71d79b23-4143-4359-a62e-489a27597b23|EcZachly       |12                |12                    |625.0                     |4                   |10                 |3                  |1      |
    |71d79b23-4143-4359-a62e-489a27597b23|johnsnake04    |13                |13                    |605.0                     |2                   |9                  |6                  |0      |
    |71d79b23-4143-4359-a62e-489a27597b23|Super Mac Bros |13                |12                    |595.0                     |2                   |15                 |2                  |1      |
    |71d79b23-4143-4359-a62e-489a27597b23|Wingspear0k8   |21                |18                    |822.8789710998535         |0                   |11                 |5                  |0      |
    |71d79b23-4143-4359-a62e-489a27597b23|JakeWilson801  |19                |18                    |865.0                     |1                   |9                  |1                  |1      |
    |71d79b23-4143-4359-a62e-489a27597b23|xTOTIx12       |6                 |6                     |274.90775299072266        |1                   |13                 |8                  |0      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|EcZachly       |9                 |9                     |410.0                     |1                   |10                 |5                  |1      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|De Spelbreker  |10                |8                     |385.00000190734863        |1                   |7                  |4                  |0      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|xXJointProXx NL|15                |14                    |615.0                     |0                   |8                  |1                  |0      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|Stormbane321   |9                 |9                     |450.0                     |0                   |13                 |6                  |1      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|deVerdeler     |13                |12                    |580.0                     |1                   |10                 |2                  |0      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|TaubeGraph07   |5                 |5                     |242.49999952316284        |0                   |14                 |8                  |1      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|derkarl15      |8                 |6                     |305.0                     |1                   |13                 |7                  |1      |
    |fc3cefc0-954a-456e-9b18-6f92f03b4037|Skainai sama   |12                |12                    |565.0                     |2                   |6                  |3                  |0      |
    |322ae96f-7c46-4504-95a2-d8d0a6cfc032|Acerr1         |15                |5                     |2232.053300857544         |7                   |14                 |4                  |0      |
    |322ae96f-7c46-4504-95a2-d8d0a6cfc032|Outlaw Fo LifE |13                |1                     |2506.9466705322266        |18                  |16                 |7                  |1      |
    |322ae96f-7c46-4504-95a2-d8d0a6cfc032|ILLICIT 117    |6                 |4                     |743.8497371673584         |8                   |19                 |1                  |0      |
    |322ae96f-7c46-4504-95a2-d8d0a6cfc032|Darknight 1993 |21                |5                     |2009.4742918014526        |13                  |16                 |6                  |1      |
    +------------------------------------+---------------+------------------+----------------------+--------------------------+--------------------+-------------------+-------------------+-------+
    only showing top 20 rows
    


matches
a row for every match


```python
file_path = "matches.csv"
```


```python
match_df = spark.read.csv(file_path, header=True, inferSchema=True)
```


```python
match_df.printSchema()
```

    root
     |-- match_id: string (nullable = true)
     |-- mapid: string (nullable = true)
     |-- is_team_game: boolean (nullable = true)
     |-- playlist_id: string (nullable = true)
     |-- game_variant_id: string (nullable = true)
     |-- is_match_over: boolean (nullable = true)
     |-- completion_date: timestamp (nullable = true)
     |-- match_duration: string (nullable = true)
     |-- game_mode: string (nullable = true)
     |-- map_variant_id: string (nullable = true)
    



```python
match_df.show(truncate=False)
```

    +------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+
    |match_id                            |mapid                               |is_team_game|playlist_id                         |game_variant_id                     |is_match_over|completion_date    |match_duration|game_mode|map_variant_id                      |
    +------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+
    |11de1a94-8d07-4162-9f5f-d3cc753c811c|c7edbf0f-f206-11e4-aa52-24be05e24f7e|true        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|true         |2016-02-22 00:00:00|NULL          |NULL     |NULL                                |
    |d3643e71-3e51-43e6-a200-f4a7f306ac12|cb914b9e-f206-11e4-b447-24be05e24f7e|false       |d0766624-dbd7-4536-ba39-2d890a6143a9|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-02-14 00:00:00|NULL          |NULL     |NULL                                |
    |d78d2aae-36e4-48ac-a3b5-6d4d90f90ace|c7edbf0f-f206-11e4-aa52-24be05e24f7e|true        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|true         |2016-03-24 00:00:00|NULL          |NULL     |55e5ee2e-88df-4657-b9ae-b6ec7ca64614|
    |b440069e-ec5f-4f51-bdd1-bc0bc7fe1195|c7edbf0f-f206-11e4-aa52-24be05e24f7e|true        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|true         |2015-12-23 00:00:00|NULL          |NULL     |ec3eef73-13e3-4d4b-a922-cc195109a842|
    |1dd475fc-ee6b-4e1d-8140-c44d03812076|c93d708f-f206-11e4-a815-24be05e24f7e|true        |0e39ead4-383b-4452-bbd4-babb7becd82e|42f97cca-2cb4-497a-a0fd-ceef1ba46bcc|true         |2016-04-07 00:00:00|NULL          |NULL     |NULL                                |
    |848f02ad-72ef-4792-9914-9673245c5f07|cbcea2c0-f206-11e4-8c4a-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-17 00:00:00|NULL          |NULL     |NULL                                |
    |e207adc1-4d7a-43ab-9854-071d7e7b68ba|cc74f4e1-f206-11e4-ad66-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-04-05 00:00:00|NULL          |NULL     |NULL                                |
    |1fb5c2ec-ca60-4342-826f-1ab60ea06ca2|ca737f8f-f206-11e4-a7e2-24be05e24f7e|true        |bc0f8ad6-31e6-4a18-87d9-ad5a2dbc8212|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2015-12-16 00:00:00|NULL          |NULL     |NULL                                |
    |54f1cbd2-2be6-4d5f-bd9e-24f1361d66f7|cbcea2c0-f206-11e4-8c4a-24be05e24f7e|NULL        |892189e9-d712-4bdb-afa7-1ccab43fbed4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|NULL         |2016-02-04 00:00:00|NULL          |NULL     |7108c409-6d1e-41d1-aca2-53b5218fbc3d|
    |9e079488-1355-4c61-8acd-b8667bc48caf|c74c9d0f-f206-11e4-8330-24be05e24f7e|true        |0bcf2be1-3168-4e42-9fb5-3551d7dbce77|b45854a7-e6e1-4a9c-9104-139934511779|true         |2015-11-22 00:00:00|NULL          |NULL     |1c632c30-3994-4443-aa6b-41d58352eb60|
    |e17af58c-ee3d-4a7f-9319-ad23bb4cada0|cbcea2c0-f206-11e4-8c4a-24be05e24f7e|NULL        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|NULL         |2016-02-04 00:00:00|NULL          |NULL     |7108c409-6d1e-41d1-aca2-53b5218fbc3d|
    |22027c64-d45a-48a7-8c41-3a800fb31276|ca737f8f-f206-11e4-a7e2-24be05e24f7e|NULL        |d0766624-dbd7-4536-ba39-2d890a6143a9|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|NULL         |2016-02-04 00:00:00|NULL          |NULL     |1ebdebca-e1f8-48f5-9bdd-3723bc431c9a|
    |8483565b-14d1-4600-beda-8bd1bbdcdeb0|c7805740-f206-11e4-982c-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-02-04 00:00:00|NULL          |NULL     |b5f6104e-3a99-438e-af4e-8a4861ed56a0|
    |bc8f0d7a-cf4b-4a27-bed4-997fe47edcb8|c7edbf0f-f206-11e4-aa52-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-02-04 00:00:00|NULL          |NULL     |7248ebbd-cd40-4563-863f-053004074973|
    |f60f9d91-42c9-4a08-b702-b9570432fd59|cdb934b0-f206-11e4-8810-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-02-04 00:00:00|NULL          |NULL     |NULL                                |
    |ad4a5b9d-7127-4047-b9ab-101bb6008c81|c7edbf0f-f206-11e4-aa52-24be05e24f7e|true        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|true         |2016-02-13 00:00:00|NULL          |NULL     |d591e5ba-a0db-4bfa-a376-2062667c8134|
    |f44c9997-eb6f-4d62-bbd4-241351d84f5c|ce1dc2de-f206-11e4-a646-24be05e24f7e|true        |0504ca3c-de41-48f3-b9c8-3aab534d69e5|b0df8938-0fb6-42ee-846f-a0c3593344d5|true         |2016-02-28 00:00:00|NULL          |NULL     |d5a6277a-96d5-499b-aa52-94935ca39fad|
    |5d8c7557-5347-4514-82db-a4a605aefc84|cdb934b0-f206-11e4-8810-24be05e24f7e|NULL        |d0766624-dbd7-4536-ba39-2d890a6143a9|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|NULL         |2016-02-04 00:00:00|NULL          |NULL     |NULL                                |
    |89b24a08-8588-4645-8ee8-bc72772b78b3|cdb934b0-f206-11e4-8810-24be05e24f7e|NULL        |d0766624-dbd7-4536-ba39-2d890a6143a9|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|NULL         |2016-02-04 00:00:00|NULL          |NULL     |NULL                                |
    |01805fa5-bd7c-4624-a66c-1405b2b0aeba|cdb934b0-f206-11e4-8810-24be05e24f7e|NULL        |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1e473914-46e4-408d-af26-178fb115de76|NULL         |2016-02-04 00:00:00|NULL          |NULL     |NULL                                |
    +------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+------------------------------------+
    only showing top 20 rows
    


medals_matches_players
a row for every medal type a player gets in a match


```python
file_path = "medals_matches_players.csv"
```


```python
medals_df = spark.read.csv(file_path, header=True, inferSchema=True)
```


```python
medals_df.printSchema()
```

    root
     |-- match_id: string (nullable = true)
     |-- player_gamertag: string (nullable = true)
     |-- medal_id: long (nullable = true)
     |-- count: integer (nullable = true)
    



```python
player_medals_df = medals_df.select("match_id", "player_gamertag", "medal_id", "count")
```


```python
player_medals_df.show(truncate=False)
```

    +------------------------------------+---------------+----------+-----+
    |match_id                            |player_gamertag|medal_id  |count|
    +------------------------------------+---------------+----------+-----+
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |3261908037|7    |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |824733727 |2    |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2078758684|2    |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2782465081|2    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3001183151|1    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3565443938|6    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3491849182|1    |
    |4a078b2f-65eb-4c61-a48d-5da5e6e65637|EcZachly       |3261908037|8    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2105198095|6    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2916014239|3    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3261908037|6    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |1351381581|2    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2838259753|1    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3354395650|1    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |298813630 |1    |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2430242797|1    |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |3261908037|9    |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |2078758684|2    |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |824733727 |1    |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |1080468863|1    |
    +------------------------------------+---------------+----------+-----+
    only showing top 20 rows
    


medals
a row for every medal type


```python
file_path = "medals.csv"
```


```python
medal_df = spark.read.csv(file_path, header=True, inferSchema=True)
```


```python
medal_df.printSchema()
```

    root
     |-- medal_id: long (nullable = true)
     |-- sprite_uri: string (nullable = true)
     |-- sprite_left: integer (nullable = true)
     |-- sprite_top: integer (nullable = true)
     |-- sprite_sheet_width: integer (nullable = true)
     |-- sprite_sheet_height: integer (nullable = true)
     |-- sprite_width: integer (nullable = true)
     |-- sprite_height: integer (nullable = true)
     |-- classification: string (nullable = true)
     |-- description: string (nullable = true)
     |-- name: string (nullable = true)
     |-- difficulty: integer (nullable = true)
    



```python
medal_types_df = medal_df.select("medal_id", "classification", "name", "description").distinct()

```


```python
medal_types_df.show(truncate=False)
```

    +----------+-----------------+------------------+----------------------------------------------------------------------------------------------------------------------+
    |medal_id  |classification   |name              |description                                                                                                           |
    +----------+-----------------+------------------+----------------------------------------------------------------------------------------------------------------------+
    |2287626681|Style            |Melee Kill        |Kill an opponent by performing a melee attack.                                                                        |
    |3718365815|Vehicles         |Phaeton Assist    |Assist a player in destroying an enemy Phaeton.                                                                       |
    |2150057864|Style            |Pounder           |Kill an opponent with a Ground Pound that has been damaged by another Ground Pound.                                   |
    |2093481574|WeaponProficiency|Snipeltaneous!    |Kill two or more enemies by using only one Sniper Rifle round.                                                        |
    |2028249938|CaptureTheFlag   |Carrier Kill      |Kill the enemy flag carrier.                                                                                          |
    |2896365521|Style            |Team Takedown     |Assist in a Team Takedown.                                                                                            |
    |2155964350|Breakout         |Fast Break        |Get a kill within the first 10 seconds of a round.                                                                    |
    |121303890 |WeaponProficiency|Rocket Kill       |Kill an opponent by using a Rocket Launcher.                                                                          |
    |2494364276|Style            |Last Shot         |Kill an enemy player with the last bullet in your magazine.                                                           |
    |2359847435|Breakout         |Extinction        |Kill every member of the opposing team in a round of Breakout.                                                        |
    |3592822316|KillingSpree     |Perfection        |Win a Slayer game without dying and with at least 15 kills.                                                           |
    |352859864 |Style            |Team Takedown     |Assist in a Team Takedown.                                                                                            |
    |2462002800|Style            |Combat Evolved    |Catch a power weapon that was blasted off of a weapon pad with a Plasma Grenade.                                      |
    |2108880282|WeaponProficiency|Snipunch          |Kill an enemy with melee after shooting them with a Sniper Rifle.                                                     |
    |1524161777|Warzone          |Legendary Takedown|Participate in a Legendary Boss Takedown.                                                                             |
    |68187090  |Breakout         |Trifecta          |Survive a three-on-one encounter.                                                                                     |
    |762229696 |Vehicles         |Banshee Destroyed |Destroy an enemy Banshee.                                                                                             |
    |2271298273|Style            |Clutch Kill       |Kill an enemy to tie or gain the lead in the final seconds.                                                           |
    |2683910456|Style            |Noob Combo        |Kill an opponent with a headshot from a Battle Rifle after depleting their shields with a fully-charged Plasma Pistol.|
    |281002471 |Vehicles         |Busted            |Kill an opponent attempting to hijack or skyjack a vehicle.                                                           |
    +----------+-----------------+------------------+----------------------------------------------------------------------------------------------------------------------+
    only showing top 20 rows
    


Build a Spark job that
Disabled automatic broadcast join with spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
Explicitly broadcast JOINs medals and maps
Bucket join match_details, matches, and medal_matches_players on match_id with 16 buckets
Aggregate the joined data frame to figure out questions like:
Which player averages the most kills per game?
Which playlist gets played the most?
Which map gets played the most?
Which map do players get the most Killing Spree medals on?
With the aggregated data set
Try different .sortWithinPartitions to see which has the smallest data size (hint: playlists and maps are both very low cardinality)


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc
from pyspark.sql.functions import broadcast
```


```python
def main():
    # Step 1: Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SparkJob") \
        .getOrCreate()
```

# Step 2: Disable Automatic Broadcast Joins


```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

# Step 3: Load the CSV Files


```python
match_details_df = spark.read.csv("match_details.csv", header=True, inferSchema=True)
matches_df = spark.read.csv("matches.csv", header=True, inferSchema=True)
medal_matches_players_df = spark.read.csv("medals_matches_players.csv", header=True, inferSchema=True)

```

# Step 4: Explicitly Broadcast Join Medals and Maps


```python
medals_df = spark.read.csv("medals.csv", header=True, inferSchema=True)
maps_df = spark.read.csv("maps.csv", header=True, inferSchema=True)

```


```python
medals_df.printSchema()
```

    root
     |-- medal_id: long (nullable = true)
     |-- sprite_uri: string (nullable = true)
     |-- sprite_left: integer (nullable = true)
     |-- sprite_top: integer (nullable = true)
     |-- sprite_sheet_width: integer (nullable = true)
     |-- sprite_sheet_height: integer (nullable = true)
     |-- sprite_width: integer (nullable = true)
     |-- sprite_height: integer (nullable = true)
     |-- classification: string (nullable = true)
     |-- description: string (nullable = true)
     |-- name: string (nullable = true)
     |-- difficulty: integer (nullable = true)
    



```python
maps_df.printSchema()
```

    root
     |-- mapid: string (nullable = true)
     |-- name: string (nullable = true)
     |-- description: string (nullable = true)
    



```python
medals_df.select("name").distinct().show()
maps_df.select("name").distinct().show()
```

    +---------------+
    |           name|
    +---------------+
    |Big Game Hunter|
    | Snipeltaneous!|
    |      Alley-Oop|
    |   Supercombine|
    |      Ball Kill|
    |           Goal|
    |      Flag Kill|
    |Mythic Takedown|
    |       Headshot|
    |   Carrier Kill|
    |Sniper Headshot|
    |         Busted|
    |  Ball Champion|
    |     Flag Joust|
    |Flagsassination|
    |     Game Saver|
    |      Road Trip|
    |      Beam Kill|
    |     Perfection|
    |           Kill|
    +---------------+
    only showing top 20 rows
    
    +-------------------+
    |               name|
    +-------------------+
    |              Urban|
    |   Meridian Station|
    |          Blue Team|
    |              Plaza|
    |     Raid on Apex 7|
    |         Overgrowth|
    |            Reunion|
    |              Array|
    |         Evacuation|
    |             Empire|
    |        Enemy Lines|
    |       The Breaking|
    |            Genesis|
    |               Eden|
    |             Osiris|
    |Sword of Sanghelios|
    |           Alliance|
    |     Breakout Arena|
    |             Summit|
    |  Battle of Sunaion|
    +-------------------+
    only showing top 20 rows
    



```python
medals_maps_joined_df = medals_df.join(
    broadcast(maps_df),
    medals_df["name"] == maps_df["name"],
    "inner"
)

```


```python
medals_matches_joined_df.show(truncate=False)
```

    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    |match_id                            |player_gamertag|medal_id  |count|medal_id  |sprite_uri                                                                                                                    |sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification   |description                                                       |name               |difficulty|
    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |3261908037|7    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |824733727 |2    |824733727 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|975        |450       |74                |74                 |1125        |899          |Style            |Distract an opponent who is then killed.                          |Distraction        |150       |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2078758684|2    |2078758684|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |300       |74                |74                 |1125        |899          |MultiKill        |Kill 2 opponents within 5 seconds of one another.                 |Double Kill        |45        |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2782465081|2    |2782465081|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|0          |675       |74                |74                 |1125        |899          |Style            |Kill an enemy who shot you first.                                 |Reversal           |115       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3001183151|1    |3001183151|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|300        |600       |74                |74                 |1125        |899          |Style            |Earn the first kill of the match.                                 |First Strike       |180       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3565443938|6    |3565443938|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|750        |450       |74                |74                 |1125        |899          |Strongholds      |Capture a Stronghold.                                             |Stronghold Captured|15        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3491849182|1    |3491849182|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |675       |74                |74                 |1125        |899          |Style            |Get a long range Grenade kill.                                    |Hail Mary          |160       |
    |4a078b2f-65eb-4c61-a48d-5da5e6e65637|EcZachly       |3261908037|8    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2105198095|6    |2105198095|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|525        |300       |74                |74                 |1125        |899          |Strongholds      |Control all Strongholds on the map.                               |Total Control      |5         |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2916014239|3    |2916014239|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |450       |74                |74                 |1125        |899          |Style            |Assist in a Team Takedown.                                        |Team Takedown      |245       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3261908037|6    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |1351381581|2    |1351381581|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|225        |150       |74                |74                 |1125        |899          |Strongholds      |Kill an enemy that is actively capturing a Stronghold.            |Stronghold Defense |20        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2838259753|1    |2838259753|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|150        |600       |74                |74                 |1125        |899          |Style            |Save a teammate by killing his attacker.                          |Protector          |145       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3354395650|1    |3354395650|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |600       |74                |74                 |1125        |899          |Strongholds      |Capture 3 Strongholds in one life.                                |Capture Spree      |10        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |298813630 |1    |298813630 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|0          |825       |74                |74                 |1125        |899          |Style            |Kill an enemy with Spartan Charge.                                |Spartan Charge     |135       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2430242797|1    |2430242797|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|675        |300       |74                |74                 |1125        |899          |KillingSpree     |Kill 5 opponents without dying.                                   |Killing Spree      |45        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |3261908037|9    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |2078758684|2    |2078758684|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |300       |74                |74                 |1125        |899          |MultiKill        |Kill 2 opponents within 5 seconds of one another.                 |Double Kill        |45        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |824733727 |1    |824733727 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|975        |450       |74                |74                 |1125        |899          |Style            |Distract an opponent who is then killed.                          |Distraction        |150       |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |1080468863|1    |1080468863|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|75         |0         |74                |74                 |1125        |899          |WeaponProficiency|Kill a player in four shots with the Battle Rifle without missing.|Perfect Kill       |45        |
    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    only showing top 20 rows
    



```python
medal_matches_players_df = spark.read.csv("medals_matches_players.csv", header=True, inferSchema=True)

```

                                                                                    


```python
# Step 2: Perform an Explicit Broadcast Join
# Broadcasting medals_df (smaller table) for better performance
medals_matches_joined_df = medal_matches_players_df.join(
    broadcast(medals_df),
    medal_matches_players_df["medal_id"] == medals_df["medal_id"],
    "inner"
)
```


```python
medals_matches_joined_df.show(truncate=False)
```

    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    |match_id                            |player_gamertag|medal_id  |count|medal_id  |sprite_uri                                                                                                                    |sprite_left|sprite_top|sprite_sheet_width|sprite_sheet_height|sprite_width|sprite_height|classification   |description                                                       |name               |difficulty|
    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |3261908037|7    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |824733727 |2    |824733727 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|975        |450       |74                |74                 |1125        |899          |Style            |Distract an opponent who is then killed.                          |Distraction        |150       |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2078758684|2    |2078758684|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |300       |74                |74                 |1125        |899          |MultiKill        |Kill 2 opponents within 5 seconds of one another.                 |Double Kill        |45        |
    |009fdac5-e15c-47c6-a202-e18ff8800ce7|EcZachly       |2782465081|2    |2782465081|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|0          |675       |74                |74                 |1125        |899          |Style            |Kill an enemy who shot you first.                                 |Reversal           |115       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3001183151|1    |3001183151|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|300        |600       |74                |74                 |1125        |899          |Style            |Earn the first kill of the match.                                 |First Strike       |180       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3565443938|6    |3565443938|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|750        |450       |74                |74                 |1125        |899          |Strongholds      |Capture a Stronghold.                                             |Stronghold Captured|15        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3491849182|1    |3491849182|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |675       |74                |74                 |1125        |899          |Style            |Get a long range Grenade kill.                                    |Hail Mary          |160       |
    |4a078b2f-65eb-4c61-a48d-5da5e6e65637|EcZachly       |3261908037|8    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2105198095|6    |2105198095|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|525        |300       |74                |74                 |1125        |899          |Strongholds      |Control all Strongholds on the map.                               |Total Control      |5         |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2916014239|3    |2916014239|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |450       |74                |74                 |1125        |899          |Style            |Assist in a Team Takedown.                                        |Team Takedown      |245       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3261908037|6    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |1351381581|2    |1351381581|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|225        |150       |74                |74                 |1125        |899          |Strongholds      |Kill an enemy that is actively capturing a Stronghold.            |Stronghold Defense |20        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2838259753|1    |2838259753|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|150        |600       |74                |74                 |1125        |899          |Style            |Save a teammate by killing his attacker.                          |Protector          |145       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |3354395650|1    |3354395650|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |600       |74                |74                 |1125        |899          |Strongholds      |Capture 3 Strongholds in one life.                                |Capture Spree      |10        |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |298813630 |1    |298813630 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|0          |825       |74                |74                 |1125        |899          |Style            |Kill an enemy with Spartan Charge.                                |Spartan Charge     |135       |
    |9169d1a3-955c-4ea9-a9a4-6d57da097660|EcZachly       |2430242797|1    |2430242797|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|675        |300       |74                |74                 |1125        |899          |KillingSpree     |Kill 5 opponents without dying.                                   |Killing Spree      |45        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |3261908037|9    |3261908037|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|375        |525       |74                |74                 |1125        |899          |WeaponProficiency|Kill an opponent by shooting them in the head.                    |Headshot           |60        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |2078758684|2    |2078758684|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|450        |300       |74                |74                 |1125        |899          |MultiKill        |Kill 2 opponents within 5 seconds of one another.                 |Double Kill        |45        |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |824733727 |1    |824733727 |https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|975        |450       |74                |74                 |1125        |899          |Style            |Distract an opponent who is then killed.                          |Distraction        |150       |
    |5ae5a459-6372-4915-93a7-a63173230f34|EcZachly       |1080468863|1    |1080468863|https://content.halocdn.com/media/Default/games/halo-5-guardians/sprites/medalspritesheet-be288ea5c0994a4e9d36f43aee7bc631.png|75         |0         |74                |74                 |1125        |899          |WeaponProficiency|Kill a player in four shots with the Battle Rifle without missing.|Perfect Kill       |45        |
    +------------------------------------+---------------+----------+-----+----------+------------------------------------------------------------------------------------------------------------------------------+-----------+----------+------------------+-------------------+------------+-------------+-----------------+------------------------------------------------------------------+-------------------+----------+
    only showing top 20 rows
    


# Step 5: Bucket Join on match_id


```python
spark = SparkSession.builder \
    .appName("BucketJoinExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()
```

    24/12/08 15:52:47 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.



```python
match_details_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.match_details_bucketed")
```


```python
matches_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.matches_bucketed")


```


```python
medal_matches_players_df.write.bucketBy(16, "match_id") \
    .format("parquet") \
    .mode("overwrite") \
    .saveAsTable("default.medal_matches_players_bucketed")

```


```python
match_details_bucketed = spark.read.table("default.match_details_bucketed")
matches_bucketed = spark.read.table("default.matches_bucketed")
medal_matches_players_bucketed = spark.read.table("default.medal_matches_players_bucketed")

```


```python
joined_df = match_details_bucketed.alias("md") \
    .join(matches_bucketed.alias("m"), col("md.match_id") == col("m.match_id")) \
    .join(medal_matches_players_bucketed.alias("mp"), col("md.match_id") == col("mp.match_id"))


```


```python
joined_df.show(truncate=False)
```

    [Stage 232:>                                                        (0 + 4) / 4]

    +------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+--------------+------------------------------------+---------------+----------+-----+
    |match_id                            |player_gamertag|previous_spartan_rank|spartan_rank|previous_total_xp|total_xp|previous_csr_tier|previous_csr_designation|previous_csr|previous_csr_percent_to_next_tier|previous_csr_rank|current_csr_tier|current_csr_designation|current_csr|current_csr_percent_to_next_tier|current_csr_rank|player_rank_on_team|player_finished|player_average_life|player_total_kills|player_total_headshots|player_total_weapon_damage|player_total_shots_landed|player_total_melee_kills|player_total_melee_damage|player_total_assassinations|player_total_ground_pound_kills|player_total_shoulder_bash_kills|player_total_grenade_damage|player_total_power_weapon_damage|player_total_power_weapon_grabs|player_total_deaths|player_total_assists|player_total_grenade_kills|did_win|team_id|match_id                            |mapid                               |is_team_game|playlist_id                         |game_variant_id                     |is_match_over|completion_date    |match_duration|game_mode|map_variant_id|match_id                            |player_gamertag|medal_id  |count|
    +------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+--------------+------------------------------------+---------------+----------+-----+
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |3261908037|11   |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |3001183151|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |824733727 |3    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |2078758684|3    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |2430242797|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |466059351 |1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |2782465081|2    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |2287626681|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Sope      |2782465081|2    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Sope      |3261908037|10   |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Sope      |2287626681|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Sope      |2078758684|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|King Sope      |824733727 |1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|mcnaeric       |3261908037|9    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|mcnaeric       |2782465081|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|mcnaeric       |2078758684|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|EXTREMENOVA    |2287626681|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|EXTREMENOVA    |3261908037|6    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|EXTREMENOVA    |2782465081|1    |
    |00169217-cca6-4b47-8df0-559ee424143f|King Terror V  |68                   |68          |1597155          |1601153 |1                |7                       |2036        |0                                |17               |1               |7                      |2039       |0                               |17              |1                  |false          |PT29.9643588S      |14                |11                    |530.6970062255859         |22                       |1                       |45.0                     |0                          |0                              |0                               |0.0                        |0.0                             |0                              |7                  |2                   |0                         |1      |1      |00169217-cca6-4b47-8df0-559ee424143f|cc040aa1-f206-11e4-a3e0-24be05e24f7e|true        |2323b76a-db98-4e03-aa37-e171cfbdd1a4|257a305e-4dd3-41f1-9824-dfe7e8bd59e1|true         |2016-03-13 00:00:00|NULL          |NULL     |NULL          |00169217-cca6-4b47-8df0-559ee424143f|EXTREMENOVA    |250435527 |1    |
    +------------------------------------+---------------+---------------------+------------+-----------------+--------+-----------------+------------------------+------------+---------------------------------+-----------------+----------------+-----------------------+-----------+--------------------------------+----------------+-------------------+---------------+-------------------+------------------+----------------------+--------------------------+-------------------------+------------------------+-------------------------+---------------------------+-------------------------------+--------------------------------+---------------------------+--------------------------------+-------------------------------+-------------------+--------------------+--------------------------+-------+-------+------------------------------------+------------------------------------+------------+------------------------------------+------------------------------------+-------------+-------------------+--------------+---------+--------------+------------------------------------+---------------+----------+-----+
    only showing top 20 rows
    


                                                                                    

# Step 6: Aggregate the Joined Data
    # 1. Player with the highest average kills per game


```python
player_avg_kills = joined_df.groupBy(col("mp.player_gamertag")) \
    .agg(avg("md.player_total_kills").alias("avg_kills")) \
    .orderBy(desc("avg_kills"))
```


```python
print("Player who averages the most kills per game:")
player_avg_kills.show(truncate=False)
```

    Player who averages the most kills per game:


    [Stage 245:========>                                                (1 + 6) / 7]

    +---------------+------------------+
    |player_gamertag|avg_kills         |
    +---------------+------------------+
    |zombiesrhunters|56.0              |
    |gimpinator14   |56.0              |
    |hibblesnbitz   |56.0              |
    |GsFurreal      |56.0              |
    |WhiteMountainDC|56.0              |
    |taurenmonk     |56.0              |
    |XoptimusprideX |56.0              |
    |I Johann117 I  |54.75             |
    |ProphetofDoom01|54.75             |
    |HisLattice1    |54.75             |
    |ForbiddenArtist|54.75             |
    |DRUNKxKANGAROO |54.75             |
    |Sexy is Back   |50.0              |
    |Darugis        |50.0              |
    |yungtrevor     |46.75             |
    |Chickensea1    |46.0              |
    |Miss Radiation |46.0              |
    |xNFG Fatel     |46.0              |
    |PrimePromethean|45.666666666666664|
    |BlightNB       |45.666666666666664|
    +---------------+------------------+
    only showing top 20 rows
    


                                                                                    

Question 2: Which playlist gets played the most?


```python
most_played_playlist = joined_df.groupBy("m.playlist_id") \
    .agg(count("*").alias("times_played")) \
    .orderBy(desc("times_played"))
```


```python
print("Playlist that gets played the most:")
most_played_playlist.show(truncate=False)
```

    Playlist that gets played the most:


    [Stage 257:>                                                        (0 + 3) / 3]

    +------------------------------------+------------+
    |playlist_id                         |times_played|
    +------------------------------------+------------+
    |f72e0ef0-7c4a-4307-af78-8e38dac3fdba|1565529     |
    |780cc101-005c-4fca-8ce7-6f36d7156ffe|1116002     |
    |0bcf2be1-3168-4e42-9fb5-3551d7dbce77|1015496     |
    |c98949ae-60a8-43dc-85d7-0feb0b92e719|824932      |
    |2323b76a-db98-4e03-aa37-e171cfbdd1a4|692342      |
    |892189e9-d712-4bdb-afa7-1ccab43fbed4|667670      |
    |f27a65eb-2d11-4965-aa9c-daa088fa5c9c|167498      |
    |355dc154-9809-4edb-8ed4-fff910c6ae9c|140006      |
    |d0766624-dbd7-4536-ba39-2d890a6143a9|138470      |
    |bc0f8ad6-31e6-4a18-87d9-ad5a2dbc8212|111073      |
    |7b7e892c-d9b7-4b03-bef8-c6a071df28ef|82723       |
    |7385b4a1-86bf-4aec-b9c2-411a6aa48633|76425       |
    |f0c9ef9a-48bd-4b24-9db3-2c76b4e23450|47813       |
    |b5d5a242-ffa5-4d88-a229-5031916be036|46411       |
    |819eb188-1a1c-48b4-9af3-283d2447ff6f|39404       |
    |d21c8381-26f1-4d65-832a-ef9fa0854eb5|37049       |
    |4b12472e-2a06-4235-ba58-f376be6c1b39|28733       |
    |5728f612-3f20-4459-98bd-3478c79c4861|28543       |
    |0504ca3c-de41-48f3-b9c8-3aab534d69e5|22502       |
    |88b7de19-113c-4beb-af7f-8553aeda3f4c|15860       |
    +------------------------------------+------------+
    only showing top 20 rows
    


                                                                                    

Question 3: Which map gets played the most?


```python
most_played_map = joined_df.groupBy("m.mapid") \
    .agg(count("*").alias("times_played")) \
    .orderBy(desc("times_played"))


```


```python
print("Map that gets played the most:")
most_played_map.show(truncate=False)

```

    Map that gets played the most:


    [Stage 265:>                                                        (0 + 4) / 4]

    +------------------------------------+------------+
    |mapid                               |times_played|
    +------------------------------------+------------+
    |c74c9d0f-f206-11e4-8330-24be05e24f7e|1445545     |
    |c7edbf0f-f206-11e4-aa52-24be05e24f7e|1435048     |
    |c7805740-f206-11e4-982c-24be05e24f7e|953278      |
    |cdb934b0-f206-11e4-8810-24be05e24f7e|396305      |
    |cb914b9e-f206-11e4-b447-24be05e24f7e|309045      |
    |ce1dc2de-f206-11e4-a646-24be05e24f7e|299736      |
    |cebd854f-f206-11e4-b46e-24be05e24f7e|298891      |
    |caacb800-f206-11e4-81ab-24be05e24f7e|291540      |
    |cd844200-f206-11e4-9393-24be05e24f7e|261162      |
    |cc040aa1-f206-11e4-a3e0-24be05e24f7e|256966      |
    |cdee4e70-f206-11e4-87a2-24be05e24f7e|244295      |
    |c7b7baf0-f206-11e4-ae9a-24be05e24f7e|204568      |
    |ca737f8f-f206-11e4-a7e2-24be05e24f7e|156631      |
    |cbcea2c0-f206-11e4-8c4a-24be05e24f7e|135375      |
    |cc74f4e1-f206-11e4-ad66-24be05e24f7e|132392      |
    |ce89a40f-f206-11e4-b83f-24be05e24f7e|65081       |
    +------------------------------------+------------+
    


                                                                                    

Question 4: Which map do players get the most Killing Spree medals on?


```python
#Filter for "Killing Spree" Medals
# Assuming "Killing Spree" corresponds to a specific `medal_id`
killing_spree_medals = medal_matches_players_df.alias("medal_matches").join(
    match_details_df.alias("match_details"),
    col("medal_matches.match_id") == col("match_details.match_id"),
    "inner"
)
```


```python
#Join the Results with Matches and Maps
killing_spree_data = killing_spree_medals.join(
    broadcast(matches_df.alias("matches")),
    col("medal_matches.match_id") == col("matches.match_id"),
    "inner"
).join(
    broadcast(maps_df.alias("maps")),
    col("matches.mapid") == col("maps.mapid"),
    "inner"
)
```


```python
#Aggregate Data by Map
most_killing_spree_map = killing_spree_data.groupBy("maps.name") \
    .agg(count("medal_matches.medal_id").alias("killing_spree_count")) \
    .orderBy(desc("killing_spree_count"))
```


```python
print("Map where players get the most Killing Spree medals:")
most_killing_spree_map.show(truncate=False)

```

    Map where players get the most Killing Spree medals:


    [Stage 902:=============================================>           (4 + 1) / 5]

    +--------------+-------------------+
    |name          |killing_spree_count|
    +--------------+-------------------+
    |alpine        |1445545            |
    |breakout arena|1435048            |
    |glacier       |953278             |
    |empire        |396305             |
    |the rig       |309045             |
    |truth         |299736             |
    |coliseum      |298891             |
    |plaza         |291540             |
    |eden          |261162             |
    |fathom        |256966             |
    |regret        |244295             |
    |parallax      |204568             |
    |NULL          |197473             |
    |overgrowth    |156631             |
    |riptide       |135375             |
    +--------------+-------------------+
    


                                                                                    

Task 5: Optimize Data Size


```python
from pyspark.sql.functions import avg, count, col, desc
```


```python
# Step 1: Aggregate the Data
aggregated_df = joined_df.groupBy("m.playlist_id", "m.mapid") \
    .agg(
        avg("md.player_total_kills").alias("avg_kills"),
        count("*").alias("total_plays")
    )
```


```python
# Step 2: Define a Function to Measure Partition Sizes
def partition_sizes(df):
    return df.rdd.mapPartitions(lambda p: [sum(len(str(row)) for row in p)]).collect()

def clean_data(df):
    return df.dropDuplicates()
```


```python
cleaned_df = clean_data(aggregated_df)
```


```python
partition_results = []
columns_to_test = ["m.playlist_id", "m.mapid"]  # Only low-cardinality columns


```


```python
for num_partitions in [4, 8, 16]:
    for column in columns_to_test:
        repartitioned_df = cleaned_df.repartition(num_partitions, col(column))
        sizes_before_sorting = partition_sizes(repartitioned_df)
        sorted_df = repartitioned_df.sortWithinPartitions(column)
        sizes_after_sorting = partition_sizes(sorted_df)
        partition_results.append({
            "column": column,
            "num_partitions": num_partitions,
            "sizes_before_sorting": sizes_before_sorting,
            "sizes_after_sorting": sizes_after_sorting,
            "total_size_before": sum(sizes_before_sorting),
            "total_size_after": sum(sizes_after_sorting)
        })

```

                                                                                    


```python
for result in partition_results:
    print(f"Column: {result['column']}, Partitions: {result['num_partitions']}")
    print(f"Partition sizes before sorting: {result['sizes_before_sorting']}")
    print(f"Partition sizes after sorting: {result['sizes_after_sorting']}")
    print(f"Total size before sorting: {result['total_size_before']} bytes")
    print(f"Total size after sorting: {result['total_size_after']} bytes")
    print()


```

    Column: m.playlist_id, Partitions: 4
    Partition sizes before sorting: [6667, 4312, 8932, 9119]
    Partition sizes after sorting: [6667, 4312, 8932, 9119]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    
    Column: m.mapid, Partitions: 4
    Partition sizes before sorting: [9459, 5348, 3703, 10520]
    Partition sizes after sorting: [9459, 5348, 3703, 10520]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    
    Column: m.playlist_id, Partitions: 8
    Partition sizes before sorting: [3708, 4312, 4491, 3291, 2959, 0, 4441, 5828]
    Partition sizes after sorting: [3708, 4312, 4491, 3291, 2959, 0, 4441, 5828]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    
    Column: m.mapid, Partitions: 8
    Partition sizes before sorting: [4428, 894, 0, 4280, 5031, 4454, 3703, 6240]
    Partition sizes after sorting: [4428, 894, 0, 4280, 5031, 4454, 3703, 6240]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    
    Column: m.playlist_id, Partitions: 16
    Partition sizes before sorting: [1186, 1773, 449, 0, 2959, 0, 1348, 2184, 2522, 2539, 4042, 3291, 0, 0, 3093, 3644]
    Partition sizes after sorting: [1186, 1773, 449, 0, 2959, 0, 1348, 2184, 2522, 2539, 4042, 3291, 0, 0, 3093, 3644]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    
    Column: m.mapid, Partitions: 16
    Partition sizes before sorting: [2212, 894, 0, 1619, 2666, 1191, 1324, 1046, 2216, 0, 0, 2661, 2365, 3263, 2379, 5194]
    Partition sizes after sorting: [2212, 894, 0, 1619, 2666, 1191, 1324, 1046, 2216, 0, 0, 2661, 2365, 3263, 2379, 5194]
    Total size before sorting: 29030 bytes
    Total size after sorting: 29030 bytes
    



```python
best_result = min(partition_results, key=lambda x: x["total_size_after"])
print(f"Best column to sortWithinPartitions: {best_result['column']} with {best_result['num_partitions']} partitions")

```

    Best column to sortWithinPartitions: m.playlist_id with 4 partitions



```python

```
