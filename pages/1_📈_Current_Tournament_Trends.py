import streamlit as st
from global_functions import create_connection

if "snowpark_session" not in st.session_state:
  session = create_connection()
  st.session_state['snowpark_session'] = session
else:
  session = st.session_state['snowpark_session']

st.write("Under Construction")