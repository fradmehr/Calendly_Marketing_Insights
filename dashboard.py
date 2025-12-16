#!/usr/bin/env python
# coding: utf-8

# dashboard.py
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates
import json

# -----------------------------
# CONFIG: Load exported data
# -----------------------------
DATA_PATH = "all_calendly_invites.csv"  # Replace with your exported CSV or Parquet

# Load data
final_df = pd.read_csv(DATA_PATH)  # or pd.read_parquet(DATA_PATH)

# ----------------------------
# Canonical datetime fields
# ----------------------------
final_df['booking_timestamp'] = pd.to_datetime(final_df['payload.scheduled_event.start_time'])
final_df['booking_date'] = final_df['booking_timestamp'].dt.date
final_df['source'] = final_df['channel']
final_df['booking_id'] = final_df['payload.scheduled_event.uri']
final_df['employee_id'] = final_df['created_by']
final_df['meeting_id'] = final_df['payload.uri']
final_df['meeting_date'] = final_df['booking_timestamp']

# -----------------------------
# Prepare meeting / user data
# -----------------------------
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

final_df['user_names'] = final_df['payload.scheduled_event.event_memberships'].apply(extract_user_names)
final_df_expanded = final_df.explode('user_names').rename(columns={'user_names': 'user_name'})
final_df_expanded = final_df_expanded.dropna(subset=['user_name'])
final_df_expanded['meeting_date'] = pd.to_datetime(final_df_expanded['meeting_date'])
final_df_expanded['week'] = final_df_expanded['meeting_date'].dt.to_period('W')
user_weekly = final_df_expanded.groupby(['user_name','week']).agg(meetings=('meeting_id','nunique')).reset_index()

# -----------------------------
# Streamlit app
# -----------------------------
st.set_page_config(layout="wide", page_title="Calendly Analytics Dashboard")
st.title("Calendly Analytics Dashboard")

tabs = st.tabs([
    "1.1 Daily Calls Booked by Channel",
    "1.2 Cost Per Booking (CPB) by Channel",
    "1.3 Bookings Trend Over Time",
    "1.4 Channel Leaderboard",
    "1.5 Booking Volume by Time / Day",
    "1.6 Meeting Load per Employee"
])

# -----------------------------
# 1.1 Daily Calls Booked by Channel
# -----------------------------
with tabs[0]:
    st.header("Daily Calls Booked by Channel")
    sources = st.multiselect("Select Channels", options=final_df['source'].dropna().unique(),
                             default=final_df['source'].dropna().unique())
    daily_bookings = final_df[final_df['source'].isin(sources)].groupby(['booking_date','source']).agg(
        bookings=('booking_id','nunique')).reset_index()

    plt.figure(figsize=(12,6))
    for src in daily_bookings['source'].unique():
        subset = daily_bookings[daily_bookings['source']==src]
        plt.plot(subset['booking_date'], subset['bookings'], marker='o', linewidth=2, label=src)
    plt.xticks(rotation=45)
    plt.xlabel("Date")
    plt.ylabel("Bookings")
    plt.title("Daily Calls Booked by Channel")
    plt.legend()
    st.pyplot(plt.gcf())

# -----------------------------
# 1.2 Cost Per Booking
# -----------------------------
with tabs[1]:
    st.header("Cost Per Booking by Channel")
    cpb_df = final_df.groupby('channel').agg(
        total_spend=('spend','sum'),
        total_bookings=('booking_id','nunique')
    ).reset_index()
    cpb_df['cpb'] = cpb_df['total_spend'] / cpb_df['total_bookings']

    plt.figure(figsize=(10,6))
    plt.bar(cpb_df['channel'], cpb_df['cpb'])
    plt.title("Cost Per Booking by Channel")
    plt.xlabel("Channel")
    plt.ylabel("CPB")
    st.pyplot(plt.gcf())

# -----------------------------
# 1.3 Bookings Trend Over Time
# -----------------------------
with tabs[2]:
    st.header("Bookings Trend Over Time by Channel")
    trend_df = final_df.groupby(['booking_date','source']).agg(bookings=('booking_id','nunique')).reset_index()

    # Line chart per channel
    plt.figure(figsize=(12,6))
    for src in trend_df['source'].dropna().unique():
        subset = trend_df[trend_df['source']==src]
        plt.plot(subset['booking_date'], subset['bookings'], marker='o', linewidth=2, label=src)
    plt.xticks(rotation=45)
    plt.xlabel("Date")
    plt.ylabel("Bookings")
    plt.title("Bookings Trend Over Time by Channel")
    plt.legend()
    st.pyplot(plt.gcf())

    # Total cumulative bookings
    total_cumulative = trend_df.groupby('booking_date')['bookings'].sum().sort_index().cumsum()
    plt.figure(figsize=(12,6))
    plt.plot(total_cumulative.index, total_cumulative.values, linewidth=3)
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.title('Total Cumulative Bookings Over Time')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Bookings')
    plt.tight_layout()
    st.pyplot(plt.gcf())

# -----------------------------
# 1.4 Channel Leaderboard
# -----------------------------
with tabs[3]:
    st.header("Channel Leaderboard")
    leaderboard = final_df.groupby('channel').agg(
        total_bookings=('booking_id','nunique'),
        total_spend=('spend','sum')
    ).reset_index()
    leaderboard['cpb'] = leaderboard['total_spend'] / leaderboard['total_bookings']
    leaderboard_sorted = leaderboard.sort_values('total_bookings', ascending=False)

    plt.figure(figsize=(10,6))
    plt.bar(leaderboard_sorted['channel'], leaderboard_sorted['total_bookings'])
    plt.xlabel("Channel")
    plt.ylabel("Bookings")
    plt.title("Top Channels by Booking Volume")
    st.pyplot(plt.gcf())

    st.dataframe(leaderboard_sorted)

# -----------------------------
# 1.5 Booking Volume by Time / Day
# -----------------------------
with tabs[4]:
    st.header("Booking Volume by Hour and Day of Week")
    final_df['hour'] = final_df['booking_timestamp'].dt.hour
    final_df['day_of_week'] = final_df['booking_timestamp'].dt.day_name()
    weekday_order = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    final_df['day_of_week'] = pd.Categorical(final_df['day_of_week'], categories=weekday_order, ordered=True)

    # Heatmap
    time_heatmap = final_df.groupby(['day_of_week','hour']).agg(bookings=('booking_id','nunique')).unstack(fill_value=0)['bookings']
    plt.figure(figsize=(12,6))
    plt.imshow(time_heatmap, aspect='auto', cmap='viridis')
    plt.colorbar(label='Bookings')
    plt.xticks(range(len(time_heatmap.columns)), time_heatmap.columns)
    plt.yticks(range(len(time_heatmap.index)), time_heatmap.index)
    plt.xlabel("Hour of Day")
    plt.ylabel("Day of Week")
    plt.title("Bookings by Hour and Day of Week")
    st.pyplot(plt.gcf())

    # Histogram
    plt.figure(figsize=(12,6))
    plt.hist(final_df['hour'], bins=24)
    plt.xlabel("Hour")
    plt.ylabel("Bookings")
    plt.title("Bookings by Hour")
    st.pyplot(plt.gcf())

    # Pie chart
    dow_counts = final_df['day_of_week'].value_counts()
    plt.figure(figsize=(8,8))
    plt.pie(dow_counts, labels=dow_counts.index, autopct='%1.1f%%')
    plt.title("Bookings by Day of Week")
    st.pyplot(plt.gcf())

# -----------------------------
# 1.6 Meeting Load per Employee
# -----------------------------
with tabs[5]:
    st.header("Meeting Load per Employee")
    users = st.multiselect("Select Users", options=user_weekly['user_name'].unique(),
                           default=user_weekly['user_name'].unique())
    filtered_weekly = user_weekly[user_weekly['user_name'].isin(users)]

    # Avg meetings
    avg_meetings = filtered_weekly.groupby('user_name')['meetings'].mean().reset_index(name='avg_meetings_per_week')
    kpi_table = filtered_weekly.groupby('user_name').agg(
        total_meetings=('meetings','sum'),
        max_meetings=('meetings','max'),
        min_meetings=('meetings','min')
    ).reset_index()

    st.subheader("Average Meetings per Week")
    plt.figure(figsize=(20,10))
    sns.barplot(data=avg_meetings, x='user_name', y='avg_meetings_per_week', palette='viridis')
    plt.xticks(rotation=90)
    st.pyplot(plt.gcf())

    st.subheader("KPI Table per User")
    st.dataframe(kpi_table)

    st.subheader("Weekly Meetings Trend")
    plt.figure(figsize=(20,10))
    for user in filtered_weekly['user_name'].unique():
        user_data = filtered_weekly[filtered_weekly['user_name']==user]
        plt.plot(user_data['week'].astype(str), user_data['meetings'], marker='o', label=user)
    plt.xticks(rotation=90)
    plt.xlabel("Week")
    plt.ylabel("Meetings")
    plt.legend(title='User', bbox_to_anchor=(1.05,1), loc='upper left')
    st.pyplot(plt.gcf())

