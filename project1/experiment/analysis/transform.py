import argparse
import csv
import os

import numpy as np
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute X, ER, EN and utilization from an experiment CSV."
    )
    parser.add_argument(
        "--filepath",
        default="experiment/data/lambda_1.2_mu_1.0.csv",
        help="Path to the raw experiment CSV.",
    )
    parser.add_argument(
        "--save-path",
        default="experiment/processed_data/results.csv",
        help="Where to append the computed metrics.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=500,
        help="Observation window length used to compute the metrics.",
    )
    parser.add_argument(
        "--lambda",
        dest="lambda_",
        type=float,
        default=1.2,
        help="Arrival rate lambda of the experiment (stored in the output).",
    )
    parser.add_argument(
        "--mu",
        type=float,
        default=1,
        help="Service rate mu of the experiment (stored in the output).",
    )
    return parser.parse_args()


args = parse_args()
filepath = args.filepath
save_path = args.save_path
duration = args.duration
lambda_ = args.lambda_
mu = args.mu

data = pd.read_csv(filepath)

ts = data['start_time'].min()
tf = data['start_time'].min() + duration
data['final_time'] = data['start_time'] + data['total_time']

# Average throughtput
X = (data['final_time'] <= tf).sum() / duration

# Average request time
er = data.loc[(data['final_time'] <= tf), 'total_time'].mean()

# Find the average requests inside the server
overlap = np.maximum(0.0, np.minimum(data['final_time'].to_numpy(), tf) - np.maximum(data['start_time'].to_numpy(), ts))
en = overlap.sum() / duration

# Compute utilization per machine
machine_ids = {
    name: i
    for i, name in enumerate(sorted(data['machine'].unique()), start=1)
}
data['machine_id'] = data['machine'].map(machine_ids)

busy_end = data['final_time'].to_numpy()
busy_start = busy_end - data['machine_time'].to_numpy()
data['busy_time'] = np.maximum(
    0.0, np.minimum(busy_end, tf) - np.maximum(busy_start, ts)
)

ui = data.groupby('machine_id')['busy_time'].sum() / duration


file_exists = os.path.isfile(save_path)
with open(save_path, "a", newline="") as f:
    writer = csv.writer(f)
    if not file_exists:
        writer.writerow(["X", "ER", "EN", "U1", "U2", "U3", "lambda", "mu"])
    writer.writerow([X, er, en, ui[1], ui[2], ui[3], lambda_, mu])
