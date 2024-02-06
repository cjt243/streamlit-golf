import streamlit as st
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
import pandas as pd

@st.cache_resource()
def create_connection() -> Session:
    conn = st.connection("snowflake")
    return conn.session()
    

def validate_session():
    try:
        session = st.session_state['snowpark_session']
        session.get_current_warehouse()
    except:
        return False
    return True

def get_session() -> Session:
    if "snowpark_session" not in st.session_state:
        session = create_connection()
        st.session_state['snowpark_session'] = session
    else:
        if validate_session():
            session = st.session_state['snowpark_session']
        else:
            session = create_connection()
            st.session_state['snowpark_session'] = session
    return session


def check_password() -> bool:
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["admin_password"] == st.secrets["admin_password"]:
            st.session_state["password_correct"] = True
            del st.session_state["admin_password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="admin_password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="admin_password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

def get_active_tournament(session: Session):
    active_tournament = session.table('GOLF_LEAGUE.ANALYTICS.TOURNAMENTS').filter((F.col('TOURNAMENT_COMPLETE') == False) & (F.col('ACTIVE_TOURNAMENT') == True)).limit(1)
    return active_tournament.select("EVENT").collect()[0][0]