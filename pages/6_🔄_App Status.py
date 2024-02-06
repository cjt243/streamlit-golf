import streamlit as st
import requests
import pandas as pd
from snowflake.snowpark import Session, functions as F
from global_functions import get_session, check_password, get_active_tournament
from cron_descriptor import get_description


session = get_session()
tournament = get_active_tournament(session)
unconfirmed_entries_df = session.table('GOLF_LEAGUE.ANALYTICS.POOL_STAGING').filter(F.col('TOURNAMENT') == tournament).count()
task_df = pd.DataFrame(session.sql('describe task GOLF_LEAGUE.RAW.LIVE_TOURNAMENT_STATS_TASK').collect())

st.title('❤️ App Health Check')


st.write(f"""         Active Tournament = ```{tournament}```
         \nPlayer Options: ```{session.table('PICK_OPTIONS').count()}```
         \nLive Stats Task Status: ```{task_df['state'][0]}```
         \nEntries Submitted: ```{unconfirmed_entries_df} / {session.table('MEMBERS').count()}```
         \nConfirmed Entries: ```{session.table('GOLF_LEAGUE.ANALYTICS.POOL').filter(F.col('TOURNAMENT') == tournament).count()} / {session.table('MEMBERS').count()}```
         \nRefresh Schedule: ```{task_df['schedule'][0]}```
         \nRefresh Description: ```{get_description(task_df['schedule'][0].replace('USING CRON','').replace('EST',''))} in EST timezone.```
         \nLast Updated Timestamp: ```{str(session.table('LIVE_TOURNAMENT_STATS_FACT').filter(F.col("EVENT_NAME") == tournament).agg({"last_updated": "max"}).collect()[0][0])}```
         """)