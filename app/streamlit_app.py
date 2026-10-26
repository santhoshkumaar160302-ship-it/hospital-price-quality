"""Hospital Price & Quality Explorer.

Pick a state and a procedure group (DRG) to see what each hospital is paid by
Medicare for it, how that compares with the state median, and how the
hospital scores on quality.
"""

import os
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Hospital Price & Quality", layout="wide")

DATA_DIR = Path(__file__).resolve().parent / "data"
# Two data sources, same SQL:
#   postgres - the live warehouse (local Docker setup; WAREHOUSE_* env vars are set)
#   parquet  - a published snapshot of the marts in app/data/ (public deployment, no database)
SOURCE = os.environ.get("DASHBOARD_SOURCE") or (
    "postgres" if (os.environ.get("WAREHOUSE_HOST") or os.environ.get("WAREHOUSE_DSN")) else "parquet"
)
SNAPSHOT_TABLES = ["mart_hospital_value", "mart_drg_price_trend"]


class NoData(Exception):
    pass


def conninfo() -> str:
    return os.environ.get("WAREHOUSE_DSN") or (
        f"host={os.environ.get('WAREHOUSE_HOST', 'localhost')} port={os.environ.get('WAREHOUSE_PORT', '5432')} "
        f"dbname={os.environ.get('WAREHOUSE_DB', 'warehouse')} user={os.environ.get('WAREHOUSE_USER', 'warehouse')} "
        f"password={os.environ.get('WAREHOUSE_PASSWORD', 'warehouse')}"
    )


@st.cache_resource
def snapshot_db():
    import duckdb

    missing = [t for t in SNAPSHOT_TABLES if not (DATA_DIR / f"{t}.parquet").exists()]
    if missing:
        raise NoData(f"snapshot files missing: {missing}")
    con = duckdb.connect()
    con.execute("CREATE SCHEMA marts")
    for t in SNAPSHOT_TABLES:
        con.execute(f"CREATE VIEW marts.{t} AS SELECT * FROM read_parquet('{DATA_DIR / (t + '.parquet')}')")
    return con


@st.cache_data(ttl=600)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    if SOURCE == "parquet":
        return snapshot_db().execute(sql.replace("%s", "?"), list(params)).df()
    import psycopg

    try:
        with psycopg.connect(conninfo()) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except psycopg.Error as e:
        raise NoData(str(e)) from e


def money(x) -> str:
    return f"${x:,.0f}" if pd.notna(x) else "–"


st.title("Hospital Price & Quality Explorer")
if SOURCE == "parquet":
    st.caption("Medicare inpatient payments vs. CMS quality ratings. Source: CMS public data, processed by an "
               "Airflow + dbt pipeline; this public demo reads a published snapshot of its output tables.")
else:
    st.caption("Medicare inpatient payments vs. CMS quality ratings. "
               "Source: CMS public data, refreshed weekly by Airflow.")

try:
    states = query("SELECT DISTINCT state FROM marts.mart_hospital_value ORDER BY state")["state"].tolist()
except NoData:
    st.warning("No data yet. Run the pipeline first (see README: `make demo` or trigger the Airflow DAG).")
    st.stop()

drgs = query("""
    SELECT drg_cd, max(drg_desc) AS drg_desc, sum(discharges) AS discharges
    FROM marts.mart_hospital_value GROUP BY drg_cd ORDER BY discharges DESC
""")
drg_label = {r.drg_cd: f"{r.drg_cd} · {r.drg_desc.capitalize()}" for r in drgs.itertuples()}

c1, c2 = st.columns([1, 3])
state = c1.selectbox("State", states, index=states.index("IN") if "IN" in states else 0)
drg = c2.selectbox("Procedure group (DRG), most common first", list(drg_label), format_func=drg_label.get)

hospitals = query("""
    SELECT hospital_name, city, overall_rating, readmission_rate_pct, discharges,
           avg_total_payment, avg_medicare_payment, price_index_vs_state, value_segment, data_year
    FROM marts.mart_hospital_value
    WHERE state = %s AND drg_cd = %s
    ORDER BY avg_total_payment
""", (state, drg))

if hospitals.empty:
    st.info("No hospitals with enough discharges for this procedure in this state.")
    st.stop()

year = int(hospitals["data_year"].iloc[0])
lo, hi = hospitals["avg_total_payment"].min(), hospitals["avg_total_payment"].max()
high_value = (hospitals["value_segment"] == "High value").sum()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Hospitals", len(hospitals))
m2.metric("Lowest avg payment", money(lo))
m3.metric("Highest avg payment", money(hi))
m4.metric("Price spread", f"{float(hi / lo):.1f}×" if lo else "–")
st.markdown(
    f"In **{state}** ({year} data), the same procedure group is paid anywhere from **{money(lo)}** to "
    f"**{money(hi)}** per stay. **{high_value}** hospital(s) are rated 4–5 stars *and* cost at or below "
    "the state median."
)

st.subheader("Price vs. star rating")
chart_df = hospitals.dropna(subset=["overall_rating"]).assign(
    price_index=lambda d: d["price_index_vs_state"].astype(float),
    stars=lambda d: d["overall_rating"].astype(int),
    payment=lambda d: d["avg_total_payment"].astype(float),
    # Fixed per-hospital vertical offset ("jitter") inside its star row, so hospitals with the
    # same rating and similar price don't sit exactly on top of each other. Derived from the
    # name, so a hospital stays in the same spot on every reload.
    jitter=lambda d: (pd.util.hash_pandas_object(d["hospital_name"], index=False) % 1000) / 1000 * 1.6 - 0.8,
)
if chart_df.empty:
    st.info("None of these hospitals has a CMS star rating.")
else:
    segments = ["High value", "High quality, higher cost", "Average",
                "Lower cost, lower quality", "Low value"]
    dots = alt.Chart(chart_df).mark_circle(opacity=0.7, stroke="white", strokeWidth=0.6).encode(
        # Axes fit the data instead of starting at 0, so dots aren't squeezed into one corner.
        x=alt.X("price_index:Q", title="Price vs. state median (1.0 = median)",
                scale=alt.Scale(zero=False, nice=False, padding=24), axis=alt.Axis(format=".2f")),
        y=alt.Y("stars:O", title="CMS star rating", sort="descending"),
        yOffset=alt.YOffset("jitter:Q", scale=alt.Scale(domain=[-1.2, 1.2])),
        order=alt.Order("discharges:Q", sort="descending"),  # small bubbles drawn on top of big ones
        color=alt.Color("value_segment:N", title="Segment", scale=alt.Scale(domain=segments)),
        size=alt.Size("discharges:Q", title="Medicare stays", scale=alt.Scale(range=[40, 600])),
        tooltip=[
            alt.Tooltip("hospital_name:N", title="Hospital"),
            alt.Tooltip("city:N", title="City"),
            alt.Tooltip("stars:O", title="Stars"),
            alt.Tooltip("payment:Q", title="Avg total payment", format="$,.0f"),
            alt.Tooltip("price_index:Q", title="vs. state median", format=".2f"),
            alt.Tooltip("discharges:Q", title="Medicare stays"),
            alt.Tooltip("value_segment:N", title="Segment"),
        ],
    )
    median_line = alt.Chart(pd.DataFrame({"x": [1.0]})).mark_rule(strokeDash=[4, 4], opacity=0.6).encode(x="x:Q")
    # Zoom/pan only along the price axis; the star axis stays fixed at 1-5.
    zoom = alt.selection_interval(bind="scales", encodings=["x"])
    st.altair_chart((median_line + dots.add_params(zoom)).properties(height=340), width="stretch")
    st.caption("Dashed line = state median price for this procedure. Left of it is cheaper; higher is better "
               "rated. Dots are spread slightly within each star row so they don't overlap. Hover a dot for "
               "details; scroll to zoom the price axis, drag to pan, double-click to reset. "
               "Bubble size = number of Medicare stays.")

st.subheader("Hospitals")
table = hospitals.drop(columns=["data_year"]).rename(columns={
    "hospital_name": "Hospital", "city": "City", "overall_rating": "Stars",
    "readmission_rate_pct": "Readmission %", "discharges": "Medicare stays",
    "avg_total_payment": "Avg total payment", "avg_medicare_payment": "Avg Medicare payment",
    "price_index_vs_state": "Price vs state median", "value_segment": "Segment",
})
st.dataframe(table, hide_index=True, width="stretch", column_config={
    "Avg total payment": st.column_config.NumberColumn(format="$%d"),
    "Avg Medicare payment": st.column_config.NumberColumn(format="$%d"),
    "Price vs state median": st.column_config.NumberColumn(format="%.2f×"),
})

st.subheader("National price trend for this procedure")
trend = query("""
    SELECT data_year, weighted_avg_total_payment, yoy_change_pct
    FROM marts.mart_drg_price_trend WHERE drg_cd = %s ORDER BY data_year
""", (drg,))
if len(trend) > 1:
    st.line_chart(trend.astype({"weighted_avg_total_payment": float}), x="data_year", y="weighted_avg_total_payment")
st.dataframe(trend, hide_index=True)

st.caption("Total payment = Medicare + patient cost-sharing + other payers. Hospitals with fewer than 11 "
           "Medicare stays for a procedure are excluded (CMS suppresses those). Ratings: CMS Overall Hospital "
           "Quality Star Rating. This is an analysis project, not medical advice.")
