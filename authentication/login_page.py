import streamlit_authenticator as stauth
import streamlit as st
import yaml
import time

def login_status():
    state = st.session_state
    with open('authentication/config.yaml') as file:
        config = yaml.safe_load(file)
    
    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    if 'authentication_status' not in state:
        state.authentication_status = False

    if state.authentication_status:
        authentication_status = state.authentication_status
    else:
        st.markdown('''
        <style>
        .css-9s5bis.edgvbvh3 {
        display: none;
        }
        </style>
        ''', unsafe_allow_html=True)

        #authenticator = stauth.Authenticate(names, usernames, hashed_passwords, "ship_recon", "admin")
            #st.write('###')
        space, login, space = st.columns([1,3,1])
        with login:
            name, authentication_status, username = authenticator.login('Login', 'main')
            time.sleep(0.2)
        state.authentication_status = authentication_status
        
    #placeholder.empty()

    if authentication_status:
        authenticator.logout('Logout', 'sidebar')
        #placeholder.empty()
    else:
        if 'submit_mas' in state:
            state.submit_mas= False
        if 'submit_ra' in state:
            state.submit_ra= False
        if 'submit_pa' in state:
            state.submit_pa= False
    return authentication_status
