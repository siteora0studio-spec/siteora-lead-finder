                    "Country": country,
                    "Public email (snippet)": "; ".join(emails),
                    "Website / Source URL": url,
                    "Phone": "",
                    "Google Maps / Social URL": "",
                    "Website status": "Needs manual verification",
                    "Email check": "Not verified",
                    "Research snippet": snippet,
                    "Found on": datetime.now().strftime("%Y-%m-%d %H:%M"),
                })
            time.sleep(1)
        except Exception as exc:
            errors.append(f"{q}: {exc}")

    df = pd.DataFrame(collected)
    if not df.empty:
        df = df.drop_duplicates(subset=["Business / Result", "Website / Source URL"])
        df = df.head(int(target))
    return df, errors

if "leads" not in st.session_state:
    st.session_state.leads = pd.DataFrame()

with st.sidebar:
    st.header("Search Settings")
    country = st.text_input("Country", "Australia")
    city = st.text_input("City", "Brisbane")
    category = st.text_input("Business category", "Barbershops")
    target = st.number_input("Maximum results", min_value=5, max_value=50, value=20, step=5)
    st.divider()
    st.caption("TinyFish API key is read from Streamlit Secrets. Never paste it into this app or GitHub.")

tab_search, tab_leads, tab_export = st.tabs(["🔎 Find Leads", "📋 My Leads", "📤 Export"])

with tab_search:
    st.subheader("Find potential clients")
    st.write("Searches public web results for businesses matching your location and category.")
    if st.button("🚀 Find Leads", type="primary", use_container_width=True):
        try:
            api_key = st.secrets["TINYFISH_API_KEY"]
        except Exception:
            st.error("TINYFISH_API_KEY is missing. Add it in your Streamlit app's Secrets and save.")
        else:
            with st.spinner("Searching public web results with TinyFish..."):
                try:
                    df, errors = run_discovery(city, country, category, target, api_key)
                    if not df.empty:
                        st.session_state.leads = pd.concat(
                            [st.session_state.leads, df], ignore_index=True
                        ).drop_duplicates(subset=["Business / Result", "Website / Source URL"])
                        st.success(f"Found {len(df)} candidate results.")
                    else:
                        st.warning("No results were returned. Try a broader category or another city.")
                    for err in errors:
                        st.warning(err)
                except requests.RequestException as exc:
                    st.error(f"Network/API error: {exc}")
                except Exception as exc:
                    st.error(f"Search failed: {exc}")

    st.info(
        "Important: Search results are leads to review, not confirmed businesses without websites. "
        "A source URL may be a directory or social profile. Open each result and verify the business "
        "and whether it has an official website before contacting it."
    )

with tab_leads:
    st.subheader("My Leads")
    if st.session_state.leads.empty:
        st.info("No leads yet. Run a search in Find Leads, or upload an existing Excel/CSV file below.")
    else:
        edited = st.data_editor(
            st.session_state.leads,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
        )
        st.session_state.leads = edited
    uploaded = st.file_uploader("Import existing leads (Excel or CSV)", type=["xlsx", "csv"])
    if uploaded is not None:
        try:
            imported = pd.read_csv(uploaded) if uploaded.name.lower().endswith(".csv") else pd.read_excel(uploaded)
            st.session_state.leads = pd.concat([st.session_state.leads, imported], ignore_index=True)
            st.success(f"Imported {len(imported)} rows.")
        except Exception as exc:
            st.error(f"Could not import file: {exc}")

with tab_export:
    st.subheader("Export your lead list")
    if st.session_state.leads.empty:
        st.info("Your lead list is empty.")
    else:
        df = st.session_state.leads
        st.metric("Saved leads", len(df))
        csv_data = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Download CSV", csv_data, "Siteora_Studio_Leads.csv", "text/csv")
        xlsx_buffer = __import__("io").BytesIO()
        with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Leads")
        st.download_button(
            "Download Excel",
            xlsx_buffer.getvalue(),
            "Siteora_Studio_Leads.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
