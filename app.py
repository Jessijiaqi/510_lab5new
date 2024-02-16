import streamlit as st
import pandas as pd
import pandas.io.sql as sqlio
import altair as alt
import folium
from streamlit_folium import st_folium


from db import conn_str

st.title("Seattle Events")



df = sqlio.read_sql_query("SELECT * FROM events", conn_str)
st.altair_chart(
    alt.Chart(df).mark_bar().encode(x="count()", y=alt.Y("category").sort('-x')).interactive(),
    use_container_width=True,
)

df['month'] = df['date'].dt.month
st.altair_chart(
    alt.Chart(df).mark_bar().encode(
        x='month:O',
        y='count():Q',
        tooltip=['month', 'count()']
    ).properties(title="Number of Events by Month").interactive(),
    use_container_width=True
)

df['day_of_week'] = df['date'].dt.day_name()
st.altair_chart(
    alt.Chart(df).mark_bar().encode(
        x=alt.X('day_of_week:O', sort=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']),
        y='count():Q',
        tooltip=['day_of_week', 'count()']
    ).properties(title="Number of Events by Day of the Week").interactive(),
    use_container_width=True
)



category = st.selectbox("Select a category", df['category'].unique())



# Dropdown to filter by location
selected_location = st.selectbox("Select a location", df['location'].unique())
df = df[df['location'] == selected_location]

# Convert the 'date' column to datetime objects, including timezone
df['date'] = pd.to_datetime(df['date'], utc=True)

# Create a list of unique dates for the dropdown, formatted as strings
dates = df['date'].dt.date.unique()
dates.sort()  # Sort the dates
date_options = [date.strftime('%Y-%m-%d') for date in dates]  # Format dates as strings

# Dropdowns to select start and end date for the range
start_date = st.selectbox("Select the start date", date_options, index=0)
end_date = st.selectbox("Select the end date", date_options, index=len(date_options)-1)

# Filter the dataframe for the range between the selected start and end date
df_filtered = df[(df['date'].dt.date >= pd.to_datetime(start_date).date()) &
                 (df['date'].dt.date <= pd.to_datetime(end_date).date())]

# Display the filtered DataFrame
st.write(df_filtered)

# Map setup with folium
m = folium.Map(location=[47.6062, -122.3321], zoom_start=12)

# Add markers to the map for filtered events
for index, event in df.iterrows():
    # Here, you should parse the 'geolocation' data to extract latitude and longitude
    # Assuming the 'geolocation' column contains strings formatted as "longitude,latitude"
    if pd.notnull(event['geolocation']):
        longitude, latitude = map(float, event['geolocation'].split(','))
        folium.Marker([latitude, longitude], popup=event['title']).add_to(m)
st_folium(m, width=1200, height=600)

# df = df[df['category'] == category]
# st.write(df)
