# Solutions

## My approach

Flattened the json data, stored it in a suitable format(csv) for staging, and denormalized the data before storing in the datawarehouse(BigQuery). The tables are :

Dimension Modelling: Stored each dimension in its own table.

- `activity_data`: Contains information about the users, activity details, plan details, workout id, record_type and week of plan.
- `lap_data`: Contains information about lap details, linked to the user via the `activity_id` column.
- `step_data`: Information about the user's step and pace, linked to the user via the `activity_id` column.
- `waypoint_data`: Information about waypoint for every activity.
- `workout_metadata`: Metadata of the workout (`activity_id`, `workout_type`, `run_type`, `distance`, `5K time`, `workout date`).

Carried out the following transformation:
- Replacing the (:) with a (.) in the "pace text columns" and converted to float data type
- Created a column "lap order" in Lap data and steps data which corresponds to each step in stepsV2


## Solutions
>> All queries below were ran in BigQuery

## Answers to Questions:
1. How much did a user beat/miss their pace targets by on average?
    - `user-1` beat their average pace target by 2.26 
    - `user-2` beat their average pace target by 3.23
```sql
SELECT
  user_id,
  AVG(diff) AS avg_pace_diff
FROM (
  SELECT
    b.activity_id,
    user_id,
    pace_average_mps,
    pace_average_text,
    (pace_average_mps - pace_average_text) AS diff
  FROM
    `k8s-sent-deployment-337205.runna.step_data` a
  JOIN
    `k8s-sent-deployment-337205.runna.activity_data` b
  ON
    a.activity_id = b.activity_id
  WHERE
    target_type = 'PACE' )
GROUP BY
  user_id
```
2. How does this workout distance compare to their workouts in the previous week of their plan?
    - There is no record for `user-2` in previous week
    - `user-1` ran a total of **5.5km** in week 5 while a total distance of **6.2km** was ran in week 6.
```sql
SELECT
  a.activity_id,
  b.user_id,
  EXTRACT(WEEK
  FROM
    TIMESTAMP_MILLIS(start_timestamp)) AS week,
  week_of_plan,
  ROUND(SUM(a.distance)/1000, 1) AS total_distance,
  c.distance AS plan_distance
FROM
  `k8s-sent-deployment-337205.runna.lap_data` a
JOIN
  `k8s-sent-deployment-337205.runna.activity_data` b
ON
  a.activity_id = b.activity_id
JOIN
  `k8s-sent-deployment-337205.runna.workout_metadata` c
ON
  b.activity_id = c.activity_id
GROUP BY 1, 2, 3, 4, 6
```
3. How did this user perform compared with other users in this same workout?
    - `user-2` ran 5.0km in a total time of ~33 minutes
    - `user-1` ran 5.5km in a total time of ~36 minutes
```sql
SELECT
  a.activity_id,
  b.user_id,
  EXTRACT(WEEK
  FROM
    TIMESTAMP_MILLIS(start_timestamp)) AS week,
  week_of_plan,
  ROUND(SUM(a.distance)/1000, 1) AS total_distance,SUM(total_time) AS total_time,
  c.distance AS plan_distance
FROM
  `k8s-sent-deployment-337205.runna.lap_data` a
JOIN
  `k8s-sent-deployment-337205.runna.activity_data` b
ON
  a.activity_id = b.activity_id
JOIN
  `k8s-sent-deployment-337205.runna.workout_metadata` c
ON
  b.activity_id = c.activity_id
GROUP BY 1, 2, 3, 4, 7
```
4. In the last 6 months, how many TEMPO sessions have been completed?
    - In the last 6 months a total of 0 TEMPO sessions were completed. 
        - `user-1`: 0 sessions
        - `user-2`: 0 sessions
```sql
SELECT
  b.activity_id,
  run_type,
  user_id
FROM
  `k8s-sent-deployment-337205.runna.workout_metadata` a
JOIN
  `k8s-sent-deployment-337205.runna.activity_data` b
ON
  a.activity_id = b.activity_id
WHERE
  run_type = 'TEMPO' and planned_workout_date = DATE_SUB(current_date(), INTERVAL 6 month)
```

#### Run Tests

```python
python -m unittest discover -p "test_*.py"
```

#### Scripts
```python
export GOOGLE_APPLICATION_CREDENTIALS = "KEY_PATH"
python workout_importer.py 
python load_multiple_csv.py
```

