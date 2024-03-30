import pandas as pd
import streamlit as st
from snowflake.snowpark import Session, exceptions
from st_pages import Page, show_pages
from global_functions import get_session, get_active_tournament
from snowflake.snowpark import functions as F
from datetime import datetime, timedelta

# Define your pages
pages = [
    Page("Home_Leaderboard.py", "Leaderboard", "🏠"),
    Page("pages/1_📈_Current_Tournament_Trends.py"),
    Page("pages/2_🏆_Trophy_Room.py"),
    Page("pages/3_🛠️_Analysis_Tools.py"),
    Page("pages/4_✏️_Make_Your_Picks.py"),
    Page("pages/5_🔐_Admin_Tools.py"),
    Page("pages/6_🔄_App Status.py"),
]

# Display the pages in the sidebar
show_pages(pages)


session = get_session()

tournament = get_active_tournament(session)
# tournament = 'THE PLAYERS Championship'

@st.cache_data(ttl=240)
def get_data_from_snowflake(_session: Session, tournament: str):
  leaderboard_display_df = _session.table('leaderboard_display_vw').filter(F.col('TOURNAMENT') == tournament).drop(['TOURNAMENT']).to_pandas()
  picks_df = _session.table('POOL_COLUMNAR_VW').filter(F.col('TOURNAMENT') == tournament)[["ENTRY_NAME","GOLFER"]].to_pandas()
  selection_df = picks_df.groupby("GOLFER").agg({'ENTRY_NAME': 'count'}).rename(columns={'ENTRY_NAME': 'SELECTIONS'}).reset_index()
  player_df = _session.table('PLAYER_LEADERBOARD_VW').filter(F.col('EVENT_NAME') == tournament).to_pandas()
  player_leaderboard_df = selection_df.merge(player_df, left_on="GOLFER", right_on="FULL_NAME")
  last_refresh = _session.table('LIVE_TOURNAMENT_STATS_FACT').filter(F.col('EVENT_NAME') == tournament).distinct().agg(F.max("LAST_UPDATED")).collect()[0][0]
  return leaderboard_display_df, picks_df, selection_df, player_df, player_leaderboard_df, last_refresh

def highlight_golfers(row, selected_golfers):
    return ['background-color: green' if golfer in selected_golfers else '' for golfer in row]

st.write(f"# {tournament}")

# create Snowpark Dataframes
leaderboard_display_df,picks_df,selection_df,player_df,player_leaderboard_df,last_refresh = get_data_from_snowflake(session, tournament)


try:
  tournament_cut_line = int(session.table('tournaments').filter(F.col("event") == tournament).select('CUT').collect()[0][0]) # type: ignore
  cut_player_score = tournament_cut_line + 1
except TypeError:
  tournament_cut_line = 'TBD'
  cut_player_score = 'TBD'

if not leaderboard_display_df.empty:
  with st.spinner('Getting yardages...'):
      st.write('#### Member Leaderboard')
      st.write(f"""```{(last_refresh + timedelta(hours=-4)).strftime("%A %b %d %I:%M %p")} EST```""")

      leaderboard_display = leaderboard_display_df
      leaderboard_display['SELECTIONS'] = leaderboard_display['SELECTIONS'].apply(lambda x: [sel.strip() for sel in x.split(",")])
      st.dataframe(leaderboard_display.set_index('RANK'))
      st.write(f"#### Cut = {tournament_cut_line}")
      st.write(f"All golfers who miss the cut will reflect as __{cut_player_score}__ for scoring")
      st.write("")
      member_names = picks_df['ENTRY_NAME'].drop_duplicates().tolist()
      selected_member = st.selectbox('Select a Member:', member_names)

      if selected_member:
        selected_golfers = picks_df[picks_df['ENTRY_NAME'] == selected_member]['GOLFER'].to_list()

      player_leaderboard_df_styled = player_leaderboard_df[['POSITION','GOLFER','TOTAL','THRU','SELECTIONS']].sort_values('TOTAL', ascending=True).style.apply(highlight_golfers, selected_golfers=selected_golfers, axis=1)

      # make sure all numbers are formatted as integers in player_leaderboard_df_styled
      player_leaderboard_df_styled = player_leaderboard_df_styled.format({'TOTAL': '{:,.0f}','THRU': '{:,.0f}'})
      
      st.write('#### Selected Player Leaderboard')
      st.dataframe(player_leaderboard_df_styled,hide_index=True, height=825) # type: ignore


else:
  st.write(f"Whoops...the players are still warming up! {tournament} hasn't started yet...come back later!")
  st.image('assets/scottie.gif',use_column_width=True)