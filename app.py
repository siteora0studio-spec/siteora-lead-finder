import io
import re
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Siteora Lead Finder", page_icon="🔎", layout="wide")

st.title("🔎 Siteora Lead Finder")
st.caption("Discover businesses, review their public contact details, and export prospects.")

st.warning(
    "Search results are leads to review, not proof that a business has no website. "
    "Manually verify each business before contacting it."
)

def get_api_key():
    try:
        return st.secrets["TINYFISH_API_KEY"]
    except Exception:
        return ""

def search_tinyfish(query: str, location: str, api_key: str):
    """Call TinyFish Search API and return the raw response for review."""
    endpoint = "https://api.search.tinyfish.ai"
    response = requests.get(
        endpoint,
        params={"query": query, "location": location, "language": "en"},
        headers={"X-API-Key": api_key},
        timeout=45,
    )
    response.raise_for_status()
    return response.json()

def extract_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(extract_text(v) for v in value.values())
    if isinstance(value, list):
        return " ".join(extract_text(v) for v in value)
    return ""

def make_rows(payload, category, location, country):
    text = extract_text(payload)
    emails = sorted(set(re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", text, re.I)))
    rows = []
    rows.append({
        "Business name": "",
        "Category": category,
        "City / Location": location,
        "Country": country,
        "Public email": "; ".join(emails),
        "Phone": "",
        "Website": "",
        "Website status": "Needs manual verification",
        "Email check": "Not verified",
        "Source / Research data": text[:4000],
        "Found on": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    return rows

if "leads" not in st.session_state:
    st.session_state.leads = pd.DataFrame(columns=[
        "Business name", "Category", "City / Location", "Country",
        "Public email", "Phone", "Website", "Website status",
        "Email check", "Source / Research data", "Found on"
    ])

tab_find, tab_leads, tab_export = st.tabs(["🔍 Find Leads", "📋 My Leads", "⬇️ Export"])

with tab_find:
    st.subheader("Search settings")
    col1, col2 = st.columns(2)
    with col1:
        country = st.text_input("Country", value="Australia")
        location = st.text_input("City or area", value="Brisbane")
    with col2:
        category = st.text_input("Business category", value="barbershops")
        extra = st.text_input("Optional search detail", placeholder="e.g. independent businesses")

    if st.button("Find leads", type="primary"):
        api_key = get_api_key()
        if not api_key:
            st.error("TinyFish API key is missing. Add TINYFISH_API_KEY in Streamlit app Secrets.")
        else:
            search_query = f"{category} in {location}, {country} {extra} business contact email website"
            with st.spinner("Searching public web results..."):
                try:
                    result = search_tinyfish(search_query, location, api_key)
                    rows = make_rows(result, category, location, country)
                    new_df = pd.DataFrame(rows)
                    st.session_state.leads = pd.concat(
                        [st.session_state.leads, new_df], ignore_index=True
                    ).drop_duplicates(subset=["Business name", "Public email", "Source / Research data"])
                    st.success("Search request completed. Review the research data below and add verified business details.")
                    st.json(result)
                except requests.HTTPError as exc:
                    body = exc.response.text[:1500] if exc.response is not None else str(exc)
                    st.error(f"TinyFish returned an HTTP error: {body}")
                except requests.RequestException as exc:
                    st.error(f"Could not connect to TinyFish: {exc}")
                except Exception as exc:
                    st.error(f"Unexpected error: {exc}")

with tab_leads:
    st.subheader("Saved leads")
    if st.session_state.leads.empty:
        st.info("No leads saved yet. Run a search to begin.")
    else:
        edited = st.data_editor(
            st.session_state.leads,
            use_container_width=True,
            num_rows="dynamic",
            key="leads_editor",
        )
        st.session_state.leads = edited

with tab_export:
    st.subheader("Export your leads")
    if st.session_state.leads.empty:
        st.info("There are no leads to export yet.")
    else:
        csv_data = st.session_state.leads.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Download CSV", csv_data, "siteora_leads.csv", "text/csv")
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            st.session_state.leads.to_excel(writer, index=False, sheet_name="Leads")
        st.download_button(
            "Download Excel",
            excel_buffer.getvalue(),
            "siteora_leads.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
