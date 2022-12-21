# ---- LOAD PACKAGES ----
import math
from datetime import date, timedelta
import yaml

import streamlit as st
import psycopg2
import pandas as pd
import streamlit_authenticator as stauth

# ---- PAGE CONFIG ----
st.set_page_config(page_title='Shaviyani ISIC Labelling',
                   page_icon=':bar_chart:',
                   layout='wide',
                   initial_sidebar_state='collapsed')

# ---- USER AUTHENTICATION ----
with open('config.yaml') as file:
    config = yaml.safe_load(file)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

# login form and messages
name, authentication_status, username = authenticator.login('Login', 'main')

if authentication_status == False:
    st.error('Username or password is incorrect.')

if authentication_status == None:
    st.warning('Please enter your username and password.')

if authentication_status:

# ---- PULL DATA AND CASHE----
    @st.experimental_memo
    def pull_data():
        connection = psycopg2.connect(
            host="dpg-ccur4ml3t398cofk19l0-a.oregon-postgres.render.com",
            database="labelling",
            user=st.secrets["dbuser"],
            password=st.secrets["dbpw"]
        )

        with connection as conn:
            sql = "select * from sentences_sentence left join auth_user au on reviewer_id = au.id"
            data = pd.read_sql_query(sql, conn)
        return data
    data = pull_data()

# ---- SIDEBAR ----
    authenticator.logout('Logout', 'sidebar')
    st.sidebar.title(f'Hi there, {name}!')
    st.sidebar.header('Configuration for calculations')

    cutoff = st.sidebar.number_input(
        'Ignore days with n < x sentences processed',
        value=10
    )

# calculate some
    processed = data[data['status']=='processed']
    proc_grp_day = processed.groupby([processed['date'].dt.date])
    count_by_day = pd.DataFrame({'count':proc_grp_day.size()})
    days_worked = proc_grp_day.size().where(lambda x : x>cutoff).dropna().count()

    past_days = st.sidebar.slider(
        'Number of working days for processing rate',
        value=3,
        min_value=1,
        max_value=int(days_worked)
    )

    # ---- PREP VARIABLES ----
    proc_tot = processed['status'].count()
    proc_tot_per = round(proc_tot/242601*100, 1)

    p3d = proc_grp_day.size().where(lambda x : x>10).dropna().loc[: date.today() - timedelta(days = 1)].iloc[-1*past_days:]
    proc_p3d_mean = round(p3d.mean())
    proc_p3d_per = round(proc_p3d_mean/242601*100, 1)

    days_projected = math.ceil((242601 - proc_tot)/proc_p3d_mean)

    # ---- MAIN PAGE ----

    st.title('Shaviyani ISIC Labelling')
    st.write('')

    # ---- MAIN METRICS -----

    left_column, middle_column, right_column = st.columns(3)
    with left_column:
        st.metric(label="Processed in total", value=f'{proc_tot:,} ({proc_tot_per} %)')

    with middle_column:
        st.metric(
            label=f'Mean processing rate in past {past_days} days',
            value=f'{proc_p3d_mean:,} ({proc_p3d_per} %)'
        )

    with right_column:
        st.metric(
            label='Number of days worked / remaining',
            value=f'{days_worked} / {days_projected}'
        )

    # ---- BAR CHART, PROCESSED BY DAY ----
    st.write('')
    st.subheader('# sentences processed by day')
    st.bar_chart(count_by_day)

    # ---- TABLE, PROCESSED BY USER AND DAY ----
    by_user_by_day = processed.groupby([processed['date'].dt.date, 'username']).size().unstack(0, fill_value=0)
    st.write('')
    st.subheader('# sentences processed by user by day')
    st.dataframe(by_user_by_day)

    # ---- COUNT BY USER AND STATUS
    st.write('')
    st.subheader('# unprocessed/viewed sentences by user ')

    left_col, right_col = st.columns([1,3])

    with left_col:
        by_user_by_status = data.groupby(['status', 'username']).size().unstack(0, fill_value=0)
        st.dataframe(by_user_by_status)

    with right_col:
        by_user_by_status_df = data.groupby(['status', 'username'], as_index=False).size()
        st.bar_chart(by_user_by_status,y=['unprocessed','viewed'])
