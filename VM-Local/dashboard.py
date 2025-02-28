import streamlit as st
import pandas as pd
import numpy as np
import glob
import os

#####################################
# 1) Basic Page Setup
#####################################
st.set_page_config(
    page_title="Live Fraud Detection Dashboard",
    layout="wide",   # to use the full width of the browser
)
st.title("Live Fraud/Anomaly Detection Dashboard")

st.write("""
This dashboard shows anomalies flagged by the IsolationForest model **in near real-time**. 
Data is continuously polled from CSV files written by Spark Streaming, 
and visualized here with charts and tables.
""")

#####################################
# 2) Folder Where Spark Writes CSV
#####################################
CSV_FOLDER = "/home/monishaapatro/flagged_anomalies"

#####################################
# 3) Function to Read All CSV Files
#####################################
def load_anomalies_data():
    """
    Reads all CSV files in CSV_FOLDER, concatenates them,
    and returns a DataFrame with the correct column names.
    If no files or if any error, it gracefully skips.
    """
    files = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
    if not files:
        return pd.DataFrame()

    df_list = []
    for f in files:
        try:
            # Spark writes CSV with no headers, so we set header=None
            tmp_df = pd.read_csv(f, header=None)
            df_list.append(tmp_df)
        except:
            # If any file is partially written or locked, skip it
            # We do not show a big traceback, to keep it pretty
            continue

    if len(df_list) == 0:
        return pd.DataFrame()

    # Concatenate all partial CSVs
    full_df = pd.concat(df_list, ignore_index=True)

    # Rename columns to match your final spark_streaming.py output
    # If your spark_streaming writes 14 columns total (including IF_Anomaly):
    # step, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
    # type_CASH_OUT, type_DEBIT, type_PAYMENT, type_TRANSFER,
    # delta_balanceOrig, delta_balanceDest, amount_div_oldbalanceOrg, IF_Anomaly
    #
    # Adjust if you actually have only 13 columns (no IF_Anomaly) or more/less.
    full_df.columns = [
        "step",
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "type_CASH_OUT",
        "type_DEBIT",
        "type_PAYMENT",
        "type_TRANSFER",
        "delta_balanceOrig",
        "delta_balanceDest",
        "amount_div_oldbalanceOrg",
        "IF_Anomaly"
    ]

    return full_df

#####################################
# 4) "Refresh" Button
#####################################
if st.button("Refresh Data"):
    st.experimental_rerun()

#####################################
# 5) Load Data from CSV
#####################################
anomalies_df = load_anomalies_data()

if anomalies_df.empty:
    st.warning("No anomalies found yet! Check if Spark is writing files, or click 'Refresh Data'.")
    st.stop()  # end the script if no data

st.success(f"Loaded {len(anomalies_df)} total anomalies from CSV files.")

#####################################
# 6) Show the Latest Rows
#####################################
st.subheader("Latest Flagged Anomalies")
st.dataframe(anomalies_df.tail(20))  # last 20 rows

#####################################
# 7) Basic Visualizations
#####################################

# A) Distribution of "step" among anomalies
st.subheader("Anomalies by Step (Bar Chart)")
steps_count = anomalies_df["step"].value_counts().sort_index()
st.bar_chart(steps_count)

# B) Distribution of "amount" (Histogram)
st.subheader("Distribution of Fraud Amounts (Histogram)")
bins = np.histogram_bin_edges(anomalies_df["amount"], bins=20)
counts, bin_edges = np.histogram(anomalies_df["amount"], bins=bins)
hist_df = pd.DataFrame({
    "bin_mid": 0.5*(bin_edges[1:] + bin_edges[:-1]),
    "counts": counts
})
hist_df.set_index("bin_mid", inplace=True)
st.bar_chart(hist_df["counts"])

# C) Transaction Types Among Anomalies
# Summation of each dummy column
st.subheader("Transaction Types Among Anomalies (Bar Chart)")
tx_types = {
    "CASH_OUT": anomalies_df["type_CASH_OUT"].sum(),
    "DEBIT": anomalies_df["type_DEBIT"].sum(),
    "PAYMENT": anomalies_df["type_PAYMENT"].sum(),
    "TRANSFER": anomalies_df["type_TRANSFER"].sum()
}
tx_df = pd.DataFrame(list(tx_types.items()), columns=["TxType","Count"])
tx_df.set_index("TxType", inplace=True)
st.bar_chart(tx_df["Count"])

st.info("Dashboard updated. Press 'Refresh Data' to load new anomalies.")
