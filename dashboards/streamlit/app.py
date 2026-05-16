"""Live operational dashboard for shelter coordinators.

Reads Delta tables and current Kafka offsets to show real-time bed availability,
outreach hot zones, and case-update activity.
"""
import time

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st


REFRESH = 5
GOLD = "data/gold"
BRONZE_OUTREACH = "data/bronze/outreach"


def _connect():
    return duckdb.connect()


def load_gold() -> pd.DataFrame:
    return _connect().execute(
        f"SELECT * FROM delta_scan('{GOLD}/fact_service_event')"
    ).df()


def load_outreach() -> pd.DataFrame:
    try:
        return _connect().execute(
            f"SELECT * FROM delta_scan('{BRONZE_OUTREACH}') WHERE event_time > now() - INTERVAL 2 HOUR"
        ).df()
    except Exception:
        return pd.DataFrame()


st.set_page_config(page_title="HMIS Live Ops", layout="wide")
st.title("HMIS Live Operations")

placeholder = st.empty()
while True:
    fact     = load_gold()
    outreach = load_outreach()

    with placeholder.container():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Services today", f"{len(fact[fact['service_date']==pd.Timestamp.today().date()]):,}")
        c2.metric("Clients (rolling)", f"{fact['client_id_hash'].nunique():,}")
        c3.metric("Active projects",   f"{fact['project_id'].nunique():,}")
        c4.metric("Outreach (2h)",     f"{len(outreach):,}")

        st.subheader("Service mix")
        st.bar_chart(fact.groupby("service_type")["qty"].sum())

        st.subheader("Outreach hotspots (last 2h)")
        if len(outreach):
            st.pydeck_chart(pdk.Deck(
                map_style=None,
                initial_view_state=pdk.ViewState(latitude=outreach["lat"].mean(),
                                                 longitude=outreach["lon"].mean(),
                                                 zoom=11),
                layers=[pdk.Layer("HeatmapLayer", data=outreach,
                                  get_position="[lon, lat]", aggregation="MEAN")],
            ))
        else:
            st.info("No outreach events in last 2 hours.")

    time.sleep(REFRESH)
