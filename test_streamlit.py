import streamlit as st
import os

st.title("Streamlit Test")
st.write("If you can see this, Streamlit is working correctly!")
st.info("Current directory: " + os.getcwd())