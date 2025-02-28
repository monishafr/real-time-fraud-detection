#!/usr/bin/env python3

import csv
import json
import time
from kafka import KafkaProducer

# Path to full PaySim dataset
CSV_PATH = "/home/monishaapatro/Transactions.csv"

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def main():
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            # parse raw columns
            step = int(row['step'])
            amount = float(row['amount'])
            oldbalanceOrg = float(row['oldbalanceOrg'])
            newbalanceOrig = float(row['newbalanceOrig'])
            oldbalanceDest = float(row['oldbalanceDest'])
            newbalanceDest = float(row['newbalanceDest'])
            
            # create dummies for 'type'
            # (only do so if that's what you used in training)
            tx_type = row['type'].upper()
            type_CASH_OUT = 1 if tx_type == "CASH_OUT" else 0
            type_DEBIT = 1 if tx_type == "DEBIT" else 0
            type_PAYMENT = 1 if tx_type == "PAYMENT" else 0
            type_TRANSFER = 1 if tx_type == "TRANSFER" else 0

            # extra features
            delta_balanceOrig = newbalanceOrig - oldbalanceOrg
            delta_balanceDest = newbalanceDest - oldbalanceDest
            amount_div_oldbalanceOrg = (
                amount / oldbalanceOrg if oldbalanceOrg > 0 else 0.0
            )

            # also keep isFraud if you want it for evaluation
            isFraud = int(row['isFraud'])

            message = {
                "step": step,
                "amount": amount,
                "oldbalanceOrg": oldbalanceOrg,
                "newbalanceOrig": newbalanceOrig,
                "oldbalanceDest": oldbalanceDest,
                "newbalanceDest": newbalanceDest,
                "type_CASH_OUT": type_CASH_OUT,
                "type_DEBIT": type_DEBIT,
                "type_PAYMENT": type_PAYMENT,
                "type_TRANSFER": type_TRANSFER,
                "delta_balanceOrig": delta_balanceOrig,
                "delta_balanceDest": delta_balanceDest,
                "amount_div_oldbalanceOrg": amount_div_oldbalanceOrg,
                "isFraud": isFraud  # optional
            }

            producer.send('paysim_transactions', value=message)

            # ~100 transactions per second
            time.sleep(0.01)

            # progress print
            if i % 1000 == 0 and i > 0:
                print(f"Sent {i} rows so far...")

    print("Finished sending all rows.")

if __name__ == "__main__":
    main()
