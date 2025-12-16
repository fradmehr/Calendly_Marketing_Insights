# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

#%sh
#pip install streamlit

# COMMAND ----------

import boto3
import json
import pandas as pd

# ---------------------------------------------------
# CONFIG — FILL THESE IN
# ---------------------------------------------------
aws_access_key_id = ""
aws_secret_access_key = ""
region_name = "us-east-1"

bucket_name = "calendly-webhook-raw"
subfolder = "events/"   # must end with "/"

# ---------------------------------------------------
# S3 CLIENT
# ---------------------------------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

# ---------------------------------------------------
# LIST ALL JSON FILES (handles >1000 files)
# ---------------------------------------------------
json_keys = []
continuation_token = None

while True:
    kwargs = {
        "Bucket": bucket_name,
        "Prefix": subfolder
    }

    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    response = s3.list_objects_v2(**kwargs)

    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".json"):
            json_keys.append(obj["Key"])

    if response.get("IsTruncated"):
        continuation_token = response.get("NextContinuationToken")
    else:
        break

print(f"Found {len(json_keys)} JSON files")

# ---------------------------------------------------
# READ + LOAD JSON FILES
# ---------------------------------------------------
records = []

for key in json_keys:
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    raw = obj["Body"].read().decode("utf-8")
    data = json.loads(raw)
    records.append(data)

# ---------------------------------------------------
# CONVERT TO DATAFRAME (ALL FIELDS)
# ---------------------------------------------------
df = pd.json_normalize(records)

# ---------------------------------------------------
# DONE
# ---------------------------------------------------
print(df.head())
print("Rows:", len(df))
print("Columns:", len(df.columns))

# COMMAND ----------

# List of event type URLs to check
event_types_to_count = [
    "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78",
    "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098",
    "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312"
]

# Count all values in the column
all_counts = df["payload.scheduled_event.event_type"].value_counts()

# Filter only the ones we want
filtered_counts = all_counts[all_counts.index.isin(event_types_to_count)]

print(filtered_counts)

# COMMAND ----------

df

# COMMAND ----------

import requests

from datetime import datetime, timedelta
import pytz

est = pytz.timezone("US/Eastern")
yesterday = (
    datetime.now(est) - timedelta(days=1)
).strftime("%Y-%m-%d")

url = f"https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/spend_data_{yesterday}.json"

response = requests.get(url)
total_spending = response.json()  # parse JSON into a Python dict/list

df_total_spending = pd.DataFrame(total_spending)


# COMMAND ----------

channel_map = {
    "https://api.calendly.com/event_types/d639ecd3-8718-4068-955a-436b10d72c78": "facebook_paid_ads",
    "https://api.calendly.com/event_types/dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098": "youtube_paid_ads",
    "https://api.calendly.com/event_types/bb339e98-7a67-4af2-b584-8dbf95564312": "tiktok_paid_ads"
}

df["channel"] = df["payload.scheduled_event.event_type"].map(channel_map)

# COMMAND ----------

df["created_at"] = pd.to_datetime(df["created_at"]).dt.strftime("%Y-%m-%d")
final_df = pd.merge(df,df_total_spending, how = 'left', left_on=['channel', 'created_at'], right_on=['channel','date'] )

# COMMAND ----------

final_df.to_csv("all_calendly_invites.csv")

# COMMAND ----------

final_df.to_excel("all_calendly_invites.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualization

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# ----------------------------
# Canonical datetime fields
# ----------------------------
final_df['booking_timestamp'] = pd.to_datetime(
    final_df['payload.scheduled_event.start_time']
)

final_df['booking_date'] = final_df['booking_timestamp'].dt.date

# ----------------------------
# Use channel as source
# ----------------------------
final_df['source'] = final_df['channel']

# ----------------------------
# Booking identifier
# ----------------------------
final_df['booking_id'] = final_df['payload.scheduled_event.uri']

# ----------------------------
# Employee / meeting fields
# ----------------------------
final_df['employee_id'] = final_df['created_by']
final_df['meeting_id'] = final_df['payload.uri']
final_df['meeting_date'] = final_df['booking_timestamp']



# COMMAND ----------

daily_bookings = (
    final_df
    .groupby(['booking_date', 'source'])
    .agg(bookings=('booking_id', 'nunique'))
    .reset_index()
)



# COMMAND ----------

import matplotlib.dates as mdates

plt.figure(figsize=(12, 6))

for src in daily_bookings['source'].dropna().unique():
    subset = daily_bookings[daily_bookings['source'] == src]

    plt.plot(
        subset['booking_date'],
        subset['bookings'],
        marker='o',          # 🔹 makes low-volume channels visible
        linewidth=2,
        label=src
    )

# ---- Clean X-axis formatting ----
ax = plt.gca()
ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))   # every 2 days
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.xticks(rotation=45)
plt.title('Daily Calls Booked by Channel')
plt.xlabel('Date')
plt.ylabel('Bookings')
plt.legend()
plt.tight_layout()
plt.show()




# COMMAND ----------

# 1.2 Cost Per Booking (CPB) by Channel
cpb_df = (
    final_df
    .groupby('channel')
    .agg(
        total_spend=('spend', 'sum'),
        total_bookings=('booking_id', 'nunique')
    )
    .reset_index()
)

cpb_df['cpb'] = cpb_df['total_spend'] / cpb_df['total_bookings']


# COMMAND ----------

plt.figure()
plt.bar(cpb_df['channel'], cpb_df['cpb'])
plt.title('Cost Per Booking by Channel')
plt.xlabel('Channel')
plt.ylabel('CPB')
plt.show()


# COMMAND ----------

total_bookings = cpb_df['total_bookings'].sum()
total_spend = cpb_df['total_spend'].sum()
average_cpb = total_spend / total_bookings

# COMMAND ----------

# 1.3 Bookings Trend Over Time (by Channel)
trend_df = (
    final_df
    .groupby(['booking_date', 'source'])
    .agg(bookings=('booking_id', 'nunique'))
    .reset_index()
)


# COMMAND ----------

import matplotlib.dates as mdates

plt.figure(figsize=(12, 6))

for src in trend_df['source'].dropna().unique():
    subset = trend_df[trend_df['source'] == src]

    plt.plot(
        subset['booking_date'],
        subset['bookings'],
        marker='o',        # 🔹 makes low-volume channels visible
        linewidth=2,
        label=src
    )

# ---- Clean X-axis formatting ----
ax = plt.gca()
ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))   # adjust if needed
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.xticks(rotation=45)
plt.title('Bookings Trend Over Time by Channel')
plt.xlabel('Date')
plt.ylabel('Bookings')
plt.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

import matplotlib.dates as mdates

total_cumulative = (
    trend_df
    .groupby('booking_date')['bookings']
    .sum()
    .sort_index()
    .cumsum()
)

plt.figure(figsize=(12, 6))
plt.plot(
    total_cumulative.index,
    total_cumulative.values,
    linewidth=3
)

ax = plt.gca()
ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

plt.xticks(rotation=45)
plt.title('Total Cumulative Bookings Over Time')
plt.xlabel('Date')
plt.ylabel('Cumulative Bookings')
plt.tight_layout()
plt.show()


# COMMAND ----------

# 1.4 Channel Attribution Leaderboard (Volume & CPB)
leaderboard = (
    final_df
    .groupby('channel')
    .agg(
        total_bookings=('booking_id', 'nunique'),
        total_spend=('spend', 'sum')
    )
    .reset_index()
)

leaderboard['cpb'] = leaderboard['total_spend'] / leaderboard['total_bookings']


# COMMAND ----------

leaderboard_sorted = leaderboard.sort_values(
    'total_bookings', ascending=False
)

plt.figure()
plt.bar(
    leaderboard_sorted['channel'],
    leaderboard_sorted['total_bookings']
)
plt.title('Top Channels by Booking Volume')
plt.xlabel('Channel')
plt.ylabel('Bookings')
plt.show()


# COMMAND ----------

# 1.5 Booking Volume by Time Slot / Day of Week
# Hour of day
final_df['hour'] = final_df['booking_timestamp'].dt.hour

# Day name (Monday, Tuesday, ...)
final_df['day_of_week'] = final_df['booking_timestamp'].dt.day_name()

# Enforce correct weekday order (important for charts & heatmaps)
weekday_order = [
    'Monday', 'Tuesday', 'Wednesday',
    'Thursday', 'Friday', 'Saturday', 'Sunday'
]

final_df['day_of_week'] = pd.Categorical(
    final_df['day_of_week'],
    categories=weekday_order,
    ordered=True
)



# COMMAND ----------

plt.figure(figsize=(12, 6))

plt.imshow(time_heatmap, aspect='auto')

plt.colorbar(label='Bookings')

# ---- X-axis (Hour labels) ----
plt.xticks(
    ticks=range(len(time_heatmap.columns)),
    labels=time_heatmap.columns
)
plt.xlabel('Hour of Day')

# ---- Y-axis (Day name labels) ----
plt.yticks(
    ticks=range(len(time_heatmap.index)),
    labels=time_heatmap.index
)
plt.ylabel('Day of Week')

plt.title('Bookings by Hour and Day of Week')
plt.tight_layout()
plt.show()



# COMMAND ----------

plt.figure()
plt.hist(final_df['hour'], bins=24)
plt.title('Bookings by Hour')
plt.xlabel('Hour')
plt.ylabel('Bookings')
plt.show()


# COMMAND ----------

dow_counts = final_df['day_of_week'].value_counts()

plt.figure()
plt.pie(dow_counts, labels=dow_counts.index, autopct='%1.1f%%')
plt.title('Bookings by Day of Week')
plt.show()


# COMMAND ----------

# 1.6 Meeting Load per Employee
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json

# -----------------------
# Data preprocessing
# -----------------------

# Ensure meeting_date is datetime
final_df['meeting_date'] = pd.to_datetime(final_df['meeting_date'])

# Robust function to extract user_name
def extract_user_names(memberships):
    if memberships is None:
        return []
    if isinstance(memberships, list):
        return [m.get('user_name') for m in memberships if 'user_name' in m]
    if isinstance(memberships, str):
        try:
            memberships_json = json.loads(memberships.replace("'", '"'))
            return [m.get('user_name') for m in memberships_json if 'user_name' in m]
        except Exception:
            return []
    return []

# Extract user_names
final_df['user_names'] = final_df['payload.scheduled_event.event_memberships'].apply(extract_user_names)

# Explode user_names
final_df_expanded = final_df.explode('user_names').rename(columns={'user_names': 'user_name'})
final_df_expanded = final_df_expanded.dropna(subset=['user_name'])

# Add week column
final_df_expanded['week'] = final_df_expanded['meeting_date'].dt.to_period('W')

# -----------------------
# Aggregate meetings per user per week
# -----------------------
user_weekly = (
    final_df_expanded
    .groupby(['user_name', 'week'])
    .agg(meetings=('meeting_id', 'nunique'))
    .reset_index()
)

# -----------------------
# 1. Bar Chart: Avg Meetings / Week per User
# -----------------------
avg_meetings = (
    user_weekly
    .groupby('user_name')['meetings']
    .mean()
    .reset_index(name='avg_meetings_per_week')
)

plt.figure(figsize=(20,10))  # wide X, narrow Y
sns.barplot(
    data=avg_meetings,
    x='user_name',
    y='avg_meetings_per_week',
    palette='viridis'
)
plt.title('Average Meetings per Week per User')
plt.xlabel('User')
plt.ylabel('Avg Meetings / Week')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()

# -----------------------
# 2. KPI Table per User
# -----------------------
kpi_table = user_weekly.groupby('user_name').agg(
    total_meetings=('meetings', 'sum'),
    max_meetings=('meetings', 'max'),
    min_meetings=('meetings', 'min')
).reset_index()

print("KPI Table per User:")
print(kpi_table)

# -----------------------
# 3. Line Chart: Weekly Trend per User
# -----------------------
plt.figure(figsize=(20,10))  # wide X, narrow Y
for user in user_weekly['user_name'].unique():
    user_data = user_weekly[user_weekly['user_name'] == user]
    if not user_data.empty:
        plt.plot(user_data['week'].astype(str), user_data['meetings'], marker='o', label=user)

plt.title('Weekly Meetings Trend per User')
plt.xlabel('Week')
plt.ylabel('Number of Meetings')
plt.xticks(rotation=90)
plt.legend(title='User', bbox_to_anchor=(1.05, 1), loc='upper left')  # move legend outside
plt.tight_layout()
plt.show()


# COMMAND ----------

# Function to extract user_name robustly
def extract_user_names(memberships):
    """
    memberships can be:
    - a list of dicts
    - a string representation of a list of dicts
    """
    if memberships is None:
        return []
    
    # If it's already a list (not a string), just extract
    if isinstance(memberships, list):
        return [m.get('user_name') for m in memberships if 'user_name' in m]
    
    # If it's a string, try to parse as JSON
    if isinstance(memberships, str):
        try:
            memberships_json = json.loads(memberships.replace("'", '"'))
            return [m.get('user_name') for m in memberships_json if 'user_name' in m]
        except Exception:
            return []
    
    return []

# Apply the robust extraction
final_df['user_names'] = final_df['payload.scheduled_event.event_memberships'].apply(extract_user_names)

# Explode the list so each user_name gets its own row
final_df_expanded = final_df.explode('user_names').rename(columns={'user_names': 'user_name'})

# Drop rows where user_name is missing
final_df_expanded = final_df_expanded.dropna(subset=['user_name'])

# Check results
final_df_expanded.user_name.value_counts()
