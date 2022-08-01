import pickle 
from pathlib import Path 
import streamlit_authenticator as stauth

password = "456"

hashed_passwords = stauth.Hasher([password]).generate()

print(hashed_passwords)