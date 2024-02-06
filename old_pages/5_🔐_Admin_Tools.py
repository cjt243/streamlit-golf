import streamlit as st
import requests
import pandas as pd
from snowflake.snowpark import Session, functions as F
from global_functions import get_session, check_password
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder

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

    with st.expander('Pre-Tourney Checklist'):
        st.checkbox('Copy Event Name into the Secrets config')
        st.checkbox('Generate Player Pick List')
        st.checkbox('Generate Tournament Record')
        st.checkbox('Truncate POOL_STAGING table')
        st.checkbox('Set the Entry Boolean to True')


    with st.expander('Tourney Setup'):


        # Get the top_5 odds list from DataGolf
        response = get_pre_tourney_odds()

        # Extract the tournament name
        tournament_name = response.json()["event_name"]
        st.write('##### Copy for Event Name in Secrets Config')
        st.code(tournament_name)

        # produce a ranked dataframe of players for picking
        pre_tourney_golfers = response.json()["baseline_history_fit"]
        pre_tourney_df = pd.DataFrame.from_dict(pre_tourney_golfers)[["player_name","top_5"]]
        pre_tourney_df.sort_values(by=['top_5'],inplace=True,ascending=False)
        pre_tourney_df["rank"] = pre_tourney_df["top_5"].rank(ascending=False).convert_dtypes()


        st.title(tournament_name)

        st.write(pre_tourney_df[0:16])

        if st.button('Generate Pick List'):
            with st.spinner('Inserting records...'):
                create_pick_list = session.write_pandas(pre_tourney_df[["player_name","rank"]],'PICK_OPTIONS',database='GOLF_NEW',schema='RAW',auto_create_table=True,overwrite=True)
                st.write(f"Created table: {create_pick_list.table_name}")


        par = int(st.radio('Select Par',[72,71,70])) # type: ignore
        
        tournament_record = pd.DataFrame.from_dict({
            "TOURNAMENT": [tournament_name],
            "PAR": [int(par)],
            "CUT": [pd.NA]
        })

        st.write(tournament_record)

        if st.button('Create Tournament Data'):
            with st.spinner('Inserting records...'):
                check = session.table('GOLF_NEW.RAW.TOURNAMENTS').filter(F.col('TOURNAMENT') == tournament_name).count()
                if check == 0:
                    session.create_dataframe(tournament_record).write.mode("append").save_as_table('GOLF_NEW.RAW.TOURNAMENTS')
                    st.write(tournament_name,' created')
                else:
                    st.write('Error: Tournament already exists.')
        if st.button('Truncate Pool Staging',):
            with st.spinner('Clearing table!'):
                session.sql('DELETE FROM GOLF_NEW.RAW.POOL_STAGING').collect()
        
    with st.expander('Entry Manager'):
        member_reference_df = session.table('GOLF_NEW.ANALYTICS.MEMBERS_VW').to_pandas()
        unconfirmed_entries_df = session.table('GOLF_NEW.RAW.POOL_STAGING').to_pandas()
        unconfirmed_entries_df.insert(loc=1,column='MEMBER_ID',value=pd.Series(dtype='int'))

        st.dataframe(member_reference_df)
        validated_df = st.experimental_data_editor(unconfirmed_entries_df)
        
        st.write('Staged Records: ')
        st.dataframe(validated_df.loc[validated_df['MEMBER_ID'] >= 0])
        if st.button('Insert Entries'):
            session.write_pandas(validated_df.loc[validated_df['MEMBER_ID'] >= 0],'POOL',schema='RAW',database='GOLF_NEW',overwrite=False)
        st.write('Existing Entries')
        st.dataframe(session.table('GOLF_NEW.RAW.POOL').filter(F.col('TOURNAMENT') == tournament_name))
    
    with st.expander('Set Cut Line'):
        edited_df = st.experimental_data_editor(session.table('GOLF_NEW.RAW.TOURNAMENTS').filter(F.col('TOURNAMENT') == tournament_name).to_pandas()[['TOURNAMENT','CUT']])
        if st.button('Update Cut'):
            session.table('GOLF_NEW.RAW.TOURNAMENTS').update({'CUT': int(edited_df['CUT'][0])},F.col('TOURNAMENT') == tournament_name)
            if int(session.table('GOLF_NEW.RAW.TOURNAMENTS').filter(F.col('TOURNAMENT') == tournament_name).to_pandas()['CUT'][0]) == int(edited_df['CUT'][0]):
                st.write('Cut Successfully Updated')
            else:
                st.write('Cut Not Updated Yet')