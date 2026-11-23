
import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(
    page_title="Siteora Lead Finder",
    page_icon="🔎",
    layout="wide"
)

st.title("🔎 Siteora Lead Finder")
st.caption("Find businesses without websites and discover their public contact details.")

if "leads" not in st.session_state:
    st.session_state.leads = pd.DataFrame(columns=[
        "Business Name",
        "Category",
        "Country",
        "City",
        "Website",
        "Website Status",
        "Business Email",
        "Phone",
        "Facebook / Instagram",
        "Google Maps",
        "Email Status"
    ])

st.sidebar.header("Search Settings")
country = st.sidebar.text_input("Country", "Australia")
city = st.sidebar.text_input("City", "Brisbane")
category = st.sidebar.text_input("Business Category", "Barbershops")
quantity = st.sidebar.number_input(
    "Number of businesses",
    min_value=1,
    max_value=100,
    value=20
)

st.sidebar.info(
    "Lead discovery will connect to a web research provider in the next development step."
)

tab1, tab2, tab3 = st.tabs([
    "🔍 Find Leads",
    "📋 My Leads",
    "📤 Export"
])

with tab1:
    st.subheader("Discover new prospects")
    st.write(
        "Your search settings are ready. The next integration will search "
        "public business listings and inspect contact sources."
    )

    st.write("**Current search**")
    st.write(f"- Country: {country}")
    st.write(f"- City: {city}")
    st.write(f"- Category: {category}")
    st.write(f"- Target: {quantity} businesses")

    st.warning(
        "Live web discovery is not connected yet. This dashboard does not "
        "claim to have searched for or verified any businesses."
    )

    uploaded = st.file_uploader(
        "Import an existing Excel or CSV lead list",
        type=["xlsx", "csv"]
    )

    if uploaded:
        try:
            if uploaded.name.endswith(".csv"):
                data = pd.read_csv(uploaded)
            else:
                data = pd.read_excel(uploaded)

            st.session_state.leads = pd.concat(
                [st.session_state.leads, data],
                ignore_index=True
            ).drop_duplicates()

            st.success(f"Imported {len(data)} rows.")
        except Exception as error:
            st.error(f"Could not import file: {error}")

with tab2:
    st.subheader("Your prospect database")

    st.metric("Total leads", len(st.session_state.leads))

    if not st.session_state.leads.empty:
        edited = st.data_editor(
            st.session_state.leads,
            use_container_width=True,
            num_rows="dynamic"
        )
        st.session_state.leads = edited
    else:
        st.info("No leads yet. Import a spreadsheet to get started.")

with tab3:
    st.subheader("Export your leads")

    if not st.session_state.leads.empty:
        output = io.BytesIO()

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            st.session_state.leads.to_excel(
                writer,
                index=False,
                sheet_name="Siteora Leads"
            )

        st.download_button(
            "⬇️ Download Excel Spreadsheet",
            data=output.getvalue(),
            file_name="Siteora_Leads.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Import or add leads before exporting.")

st.divider()
st.caption("Siteora Studio | Lead Finder | " + datetime.now().strftime("%Y"))

