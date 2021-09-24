import pandas as pd
from haversine import haversine_vector, Unit
from tabulate import tabulate
from sklearn.cluster import DBSCAN

def query_1(cnx):
    query_a = """
              SELECT
                COUNT(*) AS 'Number of Users'
              FROM
                User
              """
    query_b = """
              SELECT
                COUNT(*) AS 'Number of Activities'
              FROM
                Activity
              """
    query_c = """
              SELECT
                COUNT(*) AS 'Number of Trakpoints'
              FROM
                TrackPoint
              """
    # Create a single table for results
    query_df = pd.read_sql_query(query_a, con=cnx)
    query_df = pd.concat([query_df, pd.read_sql_query(query_b, con=cnx)], axis=1)
    query_df = pd.concat([query_df, pd.read_sql_query(query_c, con=cnx)], axis=1)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_2(cnx):
    query = """
            SELECT
              AVG(user_activities) AS 'Average Number of Activities per User',
              MIN(user_activities) AS 'Minimum Number of Activities per User',
              MAX(user_activities) AS 'Maximum Number of Activities per User'
            FROM
              (
                SELECT
                  User.id,
                  COUNT(Activity.id) AS user_activities
                FROM
                  User
                  LEFT JOIN Activity ON User.id = Activity.user_id
                GROUP BY
                  User.id
                ORDER BY
                  User.id
              ) AS T1
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_3(cnx):
    query = """
            SELECT
              user_id,
              COUNT(*) AS user_activities
            FROM
              activity
            GROUP BY
              user_id
            ORDER BY
              user_activities DESC
            LIMIT
              10
            """
    query_df = pd.read_sql_query(query, con=cnx)
    query_df.rename(columns={'user_activities':'Number of Activities'}, inplace=True)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_4(cnx):
    query = """
            SELECT
              COUNT(
                DISTINCT(user_id)
              ) AS 'Number of Users Starting and Ending an Activity at two Different Dates'
            FROM
              Activity
            WHERE
              DATEDIFF(end_date_time, start_date_time) = 1
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_5(cnx):
    query = """
            SELECT
              user_id,
              start_date_time,
              end_date_time,
              COUNT(user_id),
              COUNT(start_date_time),
              COUNT(end_date_time)
            FROM
              Activity
            GROUP BY
              user_id,
              start_date_time,
              end_date_time
            HAVING
              COUNT(user_id) > 1
              AND COUNT(start_date_time) > 1
              AND COUNT(end_date_time) > 1
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_6(cnx):
    query = """
            SELECT
              Activity.user_id,
              TrackPoint.activity_id,
              TrackPoint.id AS trackpoint_id,
              lat,
              lon,
              date_days
            FROM
              Activity
              RIGHT JOIN TrackPoint ON TrackPoint.activity_id = activity.id
            """
    query_df = pd.read_sql_query(query, con=cnx)

    # Use DBSCAN to cluster on time
    X = ((query_df['date_days'] - query_df['date_days'].min()) *24*60*60).values.reshape(-1,1)
    eps = 60 # seconds
    min_samples = 2
    cluster = DBSCAN(eps=eps, min_samples=min_samples).fit(X)
    query_df['time_labels'] = cluster.labels_
    query_df = query_df.loc[query_df['time_labels'] != -1]

    # Use DBSCAN again to cluster on distance using the haversine distance
    close_users = []
    for tl in query_df['time_labels'].unique():
        df = query_df.loc[query_df['time_labels'] == tl, ['user_id','activity_id','lat','lon']]
        X = df[['lat','lon']].values
        eps = 100 # meters
        # divide by earth radius https://en.wikipedia.org/wiki/Earth_radius#Arithmetic_mean_radius
        eps = eps / 6371008.8
        min_samples = 2
        cluster = DBSCAN(eps=eps, min_samples=min_samples, metric="haversine").fit(X)
        df['spatial_labels'] = cluster.labels_
        df = df.loc[df['spatial_labels'] != -1]
        # Get sets of users that are close to each other
        df = df.groupby(['spatial_labels']).agg(user_id=pd.NamedAgg(column='user_id', aggfunc=frozenset))
        df = df.loc[df['user_id'].map(len)>1] # must have minimum two users per spatial cluster
        # Only append unique sets for current iteration
        close_users.append(df['user_id'].unique())

    # Find all unique sets close users and remove empty arrays
    close_users = {s for arr in close_users if arr.size > 1 for s in arr}
    # Find total number of users that have been close
    number_of_close_users = len([user for s in close_users for user in s])
    print(f"Number of close users: {number_of_close_users}")

def query_7(cnx):
    query = """
            SELECT
              DISTINCT(id) AS 'User ID'
            FROM
              User
            WHERE
              id NOT IN (
                SELECT
                  DISTINCT(user_id)
                FROM
                  Activity
                WHERE
                  transportation_mode = 'taxi'
              )
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_8(cnx):
    query = """
            SELECT
              transportation_mode,
              COUNT(
                DISTINCT(user_id)
              ) AS 'Number of Distinct Users'
            FROM
              Activity
            WHERE
              transportation_mode IS NOT NULL
            GROUP BY
              transportation_mode
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_9(cnx):
    query = """
            SELECT
              *
            FROM
              Activity
            """
    query_df = pd.read_sql_query(query, con=cnx)

    # Create relevant time columns
    query_df['month_diff'] = query_df['end_date_time'].apply(lambda x: x.month) - query_df['start_date_time'].apply(lambda x: x.month)
    query_df['year_month'] = query_df['start_date_time'].apply(lambda x: x.year).astype(str) + '-' + query_df['start_date_time'].apply(lambda x: x.month_name())
    query_df['recorded_hours'] = (query_df['end_date_time'] - query_df['start_date_time']).apply(lambda x: x.total_seconds() / 3600)

    # Find most active year-month
    year_month_ma = query_df.groupby(['year_month']).count().sort_values(by='id', ascending=False).index[0]
    # Find most active and second most active user in most active year-month
    ma_df = query_df.loc[query_df['year_month'] == year_month_ma].groupby(['user_id']).count().sort_values(by='id', ascending=False)
    user_ma_1 = ma_df.index[0]
    num_act_1 = ma_df['id'][0]
    user_ma_2 = ma_df.index[1]
    num_act_2 = ma_df['id'][1]

    # Select subset of data for most active and second most active user in
    # most active year-month
    query_df = query_df.loc[(query_df['year_month'] == year_month_ma) & (query_df['user_id'].isin([user_ma_1, user_ma_2]))]

    # Assert that these users did not record any activities that started in
    # one month and ended in another
    assert all(query_df['month_diff'] == 0)

    # Create dataframe for number of activities and number of hours logged
    result_df = query_df.groupby(['user_id']).agg(recorded_hours=pd.NamedAgg(column='recorded_hours',aggfunc=sum)).reset_index()
    d = {user_ma_1:num_act_1, user_ma_2:num_act_2}
    result_df['number_of_activities'] = result_df['user_id'].map(d)
    result_df['year_month'] = year_month_ma
    print(tabulate(result_df, headers='keys', showindex=False, tablefmt='psql'))

def query_10(cnx):
    query = """
            SELECT
              TrackPoint.activity_id,
              TrackPoint.lat,
              TrackPoint.lon
            FROM
              Activity
              RIGHT JOIN TrackPoint ON TrackPoint.activity_id = Activity.id
            WHERE
              Activity.transportation_mode = 'walk'
              AND Activity.user_id = '112'
              AND YEAR(TrackPoint.date_time) = 2008
            """
    query_df = pd.read_sql_query(query, con=cnx)
    distance_walked = 0
    for aid in query_df['activity_id'].unique():
        df = query_df.loc[query_df['activity_id']==aid].copy()
        df['dist'] = haversine_vector(df[['lat','lon']].values, df[['lat','lon']].shift().values, Unit.KILOMETERS)
        distance_walked += df['dist'].sum()
    print(f"Total distance walked: {distance_walked}")

def query_11(cnx):
    query = """
            SELECT
              user_id,
              SUM(altitude_diff) AS total_elevation_gain
            FROM
              (
                SELECT
                  trackpoint_id,
                  activity_id,
                  user_id,
                  altitude,
                  altitude - LAG(altitude) OVER (
                    PARTITION BY activity_id
                    ORDER BY
                      trackpoint_id ASC
                  ) AS altitude_diff
                FROM
                  (
                    SELECT
                      TrackPoint.id AS trackpoint_id,
                      activity_id,
                      user_id,
                      altitude
                    FROM
                      activity
                      RIGHT JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                  ) AS T1
                ORDER BY
                  trackpoint_id ASC
              ) AS T2
            WHERE
              altitude_diff > 0
            GROUP BY
              user_id
            ORDER BY
              total_elevation_gain DESC
            LIMIT
              20;
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))

def query_12(cnx):
    query = """
            SELECT
              user_id,
              COUNT(
                DISTINCT(activity_id)
              ) AS number_of_invalid_activities
            FROM
              (
                SELECT
                  trackpoint_id,
                  activity_id,
                  user_id,
                  date_time,
                  date_time - LAG(date_time) OVER (
                    PARTITION BY activity_id
                    ORDER BY
                      trackpoint_id ASC
                  ) AS date_time_diff_seconds
                FROM
                  (
                    SELECT
                      TrackPoint.id AS trackpoint_id,
                      activity_id,
                      user_id,
                      date_time
                    FROM
                      Activity
                      RIGHT JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                  ) AS T1
                ORDER BY
                  trackpoint_id ASC
              ) AS T2
            WHERE
              date_time_diff_seconds > 300
            GROUP BY
              user_id
            ORDER BY
              number_of_invalid_activities DESC
            """
    query_df = pd.read_sql_query(query, con=cnx)
    print(tabulate(query_df, headers='keys', showindex=False, tablefmt='psql'))
