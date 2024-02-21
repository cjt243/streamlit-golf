import streamlit as st
from global_functions import get_session, get_active_tournament
from snowflake.snowpark import functions as F
import plotly.express as px
import altair as alt
import pandas as pd

session = get_session()

tournament = get_active_tournament(session)

st.title(tournament)

golfer_line_df = session.table('GOLFER_TIME_SERIES_VW').filter(F.col("TOURNAMENT") == tournament)
member_line_df = golfer_line_df.group_by(F.col('ENTRY_NAME'),F.col('LAST_UPDATED')).agg(F.sum(F.col('TOTAL'))).with_column_renamed(F.col('SUM(TOTAL)'),'TOTAL')


pool_trend_local_df = member_line_df.to_pandas().sort_values(by=['ENTRY_NAME','LAST_UPDATED'] , ascending=True)

st.write('### Pool Trend')

fig1 = px.line(
    pool_trend_local_df,
    x="LAST_UPDATED",
    y="TOTAL",
    color="ENTRY_NAME",
    markers=False,
    hover_data=["ENTRY_NAME","TOTAL","LAST_UPDATED"]
)
fig1.update_layout(
    xaxis = dict(
        tickmode = 'linear'
    )
)
fig1.update_yaxes(autorange="reversed")
fig1.update_layout(legend=dict(
    orientation="h",
    yanchor="bottom",
    y=-.2,
    xanchor="right",
    x=1
))

st.plotly_chart(fig1,use_container_width=True,theme='streamlit')

