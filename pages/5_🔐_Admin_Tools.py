import streamlit as st
import requests
import pandas as pd
from snowflake.snowpark import Session, functions as F
from global_functions import get_session, check_password, get_active_tournament

def get_pre_tourney_odds() -> requests.Response:
    url = f"https://feeds.datagolf.com/preds/pre-tournament?tour=pga&odds_format=percent&key={st.secrets['api_key']}"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response

if not check_password():
    st.write('☠️ Not Authorized ☠️')
else:
    session = get_session()
    session.use_schema('ANALYTICS')
    active_tournament = get_active_tournament(session)


    tab1,tab2,tab3 = st.tabs(["Tournament Setup", "Entry Manager","Task Scheduler"])

    with tab1:

        st.write(' ## Tournament Data')
        tournaments_df = session.table('TOURNAMENTS').order_by(F.col('START_DATE').asc())

        edited_df = st.data_editor(tournaments_df,num_rows='dynamic')

        if st.button("Submit Updates"):
            session.write_pandas(edited_df,'TOURNAMENTS',overwrite=True,auto_create_table=False).collect()

        st.write('## DataGolf Current Tournament')

        response = get_pre_tourney_odds()

        # Extract the tournament name
        tournament_name = response.json()["event_name"]
        st.write('Copy for Event Name in Tournament Table')
        st.code(tournament_name)

        pre_tourney_golfers = response.json()["baseline_history_fit"]
        pre_tourney_df = pd.DataFrame.from_dict(pre_tourney_golfers)[["player_name","top_5"]]
        pre_tourney_df.sort_values(by=['top_5'],inplace=True,ascending=False)
        pre_tourney_df["rank"] = pre_tourney_df["top_5"].rank(ascending=False).convert_dtypes()

        st.write(f'## {tournament_name} Field Odds')

        st.write(pre_tourney_df[0:16])

        if st.button('Generate Pick List'):
            with st.spinner('Inserting records...'):
                create_pick_list = session.write_pandas(pre_tourney_df[["player_name","rank"]],'PICK_OPTIONS',database='GOLF_LEAGUE',schema='ANALYTICS',auto_create_table=True,overwrite=True)
                st.write(f"Created table: {create_pick_list.table_name}")
                st.write(f"{session.table('GOLF_LEAGUE.ANALYTICS.PICK_OPTIONS').count()} Records Inserted")

    with tab2:
        member_reference_df = session.table('GOLF_LEAGUE.ANALYTICS.MEMBERS').to_pandas()
        unconfirmed_entries_df = session.table('GOLF_LEAGUE.ANALYTICS.POOL_STAGING').filter(F.col('TOURNAMENT') == active_tournament).to_pandas()
        unconfirmed_entries_df.insert(loc=1,column='MEMBER_ID',value=pd.Series(dtype='int'))

        st.write('Member Reference')
        st.dataframe(member_reference_df,hide_index=True)

        st.write('Unconfirmed Entries')
        validated_df = st.data_editor(unconfirmed_entries_df, hide_index=True)
        
        st.write('Staged Records: ')
        st.dataframe(validated_df.loc[validated_df['MEMBER_ID'] >= 0])
        if st.button('Insert Entries'):
            session.write_pandas(validated_df.loc[validated_df['MEMBER_ID'] >= 0],'POOL',schema='ANALYTICS',database='GOLF_LEAGUE',overwrite=False)

        existing_entries_df = session.table('GOLF_LEAGUE.ANALYTICS.POOL').filter(F.col('TOURNAMENT') == active_tournament)
        
        st.write(f'Existing Entries: {existing_entries_df.count()}')    
        st.dataframe(existing_entries_df)

    with tab3:
        session = get_session()
        tournament = get_active_tournament(session)

        session.use_schema('RAW')
        task_df = pd.DataFrame(session.sql('SHOW TASKS;').collect()).replace({'state': {'suspended': False, 'started': True}})
        st.dataframe(task_df[['name','schedule','state']],hide_index=True)
        session.use_schema('ANALYTICS')

        st.write('#### Current Schedule')
        st.code(task_df['schedule'][0])

        example_cron = 'USING CRON 0/5 <hour range> <day range> <month number> * EST'

        st.write('Example Cron')
        st.code(example_cron)

        new_schedule = st.text_input('New schedule: ')

        if st.button('Update Schedule'):
            session.sql('ALTER TASK GOLF_LEAGUE.RAW.LIVE_TOURNAMENT_STATS_TASK SUSPEND').collect()
            session.sql('ALTER TASK GOLF_LEAGUE.RAW.TRIGGERED_TOURNAMENT_TRANSFORM_TASK SUSPEND').collect()
            session.call('GOLF_LEAGUE.RAW.ALTER_TOURNAMENT_TASK_SCHEDULE', new_schedule)
            session.sql('ALTER TASK GOLF_LEAGUE.RAW.TRIGGERED_TOURNAMENT_TRANSFORM_TASK RESUME').collect()
            session.sql('ALTER TASK GOLF_LEAGUE.RAW.LIVE_TOURNAMENT_STATS_TASK RESUME').collect()
            st.rerun()