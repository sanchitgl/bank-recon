
import streamlit as st
import base64

def page_setup():
    logo = 'images/reconcify_logo.png'

    #hide copy link icon on text
    st.markdown("""
            <style>
            .css-eczf16 {display: none}
            </style>
            """, unsafe_allow_html=True)

    #hide streamlit header, hamburger menu
    hide_streamlit_style = """
        <style>
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
        """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)     

    #Reducing padding
    st.markdown("""
            <style>
                .css-18e3th9 {
                    padding-top: 0rem;
                    }
                .css-1ufdz57 {
                    padding-top: 0rem;
                }
            </style>
            """, unsafe_allow_html=True)


    #hide image fullscreen icon
    hide_img_fs = '''
    <style>
    button[title="View fullscreen"]{
        visibility: hidden;}
    </style>
    '''
    st.markdown(hide_img_fs, unsafe_allow_html=True)

    #navbar configuration
    st.markdown('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">', unsafe_allow_html=True)
    st.markdown(
        f"""
    <nav class="navbar fixed-top navbar-expand-lg navbar-dark" style="background-color: #002878;">
    <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarNav">
        <img class="logo-img" width=220 src="data:image/png;base64,{base64.b64encode(open(logo, "rb").read()).decode()}">
    </div>
    </nav>
    """, unsafe_allow_html=True) 

    
    #sidebar configuration

    st.markdown('''
    <style>
    /* Left sidebar */
    .css-1k0ckh2.e1fqkh3o9 {
    margin-top: 5.2rem;
    }
    /* Right drop-down */
    .css-9s5bis.edgvbvh3 {
    margin-top: 4.8rem;
    }
    .css-1xtoq5p.e1fqkh3o1{
        margin-top: -4.8rem
    }
    .css-1qrvfrg.edgvbvh9{
        margin-top: -4.8rem
    }
    [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
    width: 300px;
    }
    [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
    width: 300px;
    margin-left: -300px;
    }
    </style>
    ''', unsafe_allow_html=True)

    # st.markdown(
    #     """
    #     <style>
    #     [data-testid="stSidebar"][aria-expanded="true"] > div:first-child {
    #     width: 300px;
    #     }
    #     [data-testid="stSidebar"][aria-expanded="false"] > div:first-child {
    #     width: 300px;
    #     margin-left: -300px;
    #     }
    #     </style>
    #     """,
    #     unsafe_allow_html=True
    #     )