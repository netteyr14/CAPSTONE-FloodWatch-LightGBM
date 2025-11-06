import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder

from helpers.db_connection import pool
import configparser

config = configparser.ConfigParser()
config.read('server/setting.conf')


# -----------------------------
# FETCHING DATA
# -----------------------------
def fetch_rows_upto(node_name, ts, limit=250):
    conn = pool.get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT timestamp, temperature, humidity, node_1, node_2, node_name
            FROM dht11_random_forest
            WHERE node_name=%s AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (node_name, ts, limit))
        rows = cur.fetchall()
        rows = list(reversed(rows))
        return rows
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# -----------------------------
# QUEUE MANAGEMENT
# -----------------------------
def claim_job(conn):
    cur = conn.cursor(dictionary=True)
    cur.execute("START TRANSACTION")
    cur.execute("""
        SELECT node_name, ts FROM queue_table
        WHERE status='queued'
        ORDER BY ts ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """)
    row = cur.fetchone()
    if not row:
        cur.execute("COMMIT")
        cur.close()
        return None
    cur.execute("""
        UPDATE queue_table
        SET status='processing', attempt=attempt+1
        WHERE node_name=%s AND ts=%s
    """, (row["node_name"], row["ts"]))
    cur.execute("COMMIT")
    cur.close()
    return row


def job_success(conn, node_name, ts):
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE queue_table
            SET status='done', completed_at=NOW()
            WHERE node_name=%s AND ts=%s
        """, (node_name, ts))
        conn.commit()
        cur.close()
        print(f"✅ Job success recorded for node '{node_name}' at {ts}")
    except Exception as e:
        print(f"⚠️ Failed to mark job success for {node_name}: {e}")


def job_fail(conn, node_name, ts, reason=None):
    try:
        cur = conn.cursor()
        if reason:
            cur.execute("""
                UPDATE queue_table
                SET status='failed', completed_at=NOW(), fail_reason=%s
                WHERE node_name=%s AND ts=%s
            """, (reason, node_name, ts))
        else:
            cur.execute("""
                UPDATE queue_table
                SET status='failed', completed_at=NOW()
                WHERE node_name=%s AND ts=%s
            """, (node_name, ts))
        conn.commit()
        cur.close()
        print(f"❌ Job failed for node '{node_name}' at {ts}. Reason: {reason or 'Unknown'}")
    except Exception as e:
        print(f"⚠️ Failed to mark job as failed for {node_name}: {e}")


# -----------------------------
# CLEANING / RESAMPLING
# -----------------------------
def clean_dataframe(rows):
    if rows is None or len(rows) == 0:
        empty_df = pd.DataFrame(columns=["temperature", "humidity", "node_1", "node_2", "node_name"])
        empty_df.index = pd.DatetimeIndex([], name="timestamp")
        return empty_df

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").set_index("timestamp")

    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")

    for node in ["node_1", "node_2"]:
        if node in df:
            df[node] = pd.to_numeric(df[node], errors="coerce").fillna(0).round().astype(int)
    if "node_name" in df:
        df["node_name"] = df["node_name"].astype("string")

    df = df.dropna(subset=["temperature", "humidity"])
    return df


def enforce_fixed_interval(df, frequency):
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]

    for col in ["node_1", "node_2", "node_name"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = df.resample(frequency).agg({
        "temperature": "mean",
        "humidity": "mean",
        "node_1": "last",
        "node_2": "last",
        "node_name": "last",
    })

    df[["temperature", "humidity"]] = df[["temperature", "humidity"]].interpolate(method="time")
    df = df.ffill().bfill()

    for node in ["node_1", "node_2"]:
        df[node] = pd.to_numeric(df[node], errors="coerce").fillna(0).round().astype(int)
    df["node_name"] = df["node_name"].astype("string")
    return df


# -----------------------------
# FEATURES
# -----------------------------
def make_lag_features(df, n_lags):
    feat = df.copy()
    for lag_number in range(1, n_lags + 1): # range(start, end) start at 1 and ends before 4 therefore (1,3)
        feat[f"temp_lag{lag_number}"] = feat["temperature"].shift(lag_number)
        feat[f"hum_lag{lag_number}"] = feat["humidity"].shift(lag_number)
    feat["target_next_temp"] = feat["temperature"].shift(-1)
    need_cols = [c for c in feat.columns if c.startswith("temp_lag") or c.startswith("hum_lag")] + ["target_next_temp"]
    feat = feat.dropna(subset=need_cols)
    return feat


# -----------------------------
# TRAINING
# -----------------------------
def _make_ohe():
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def train_model(df):
    min_rows = config.getint('rf_model', 'MIN_REQUIRED_ROWS')
    if len(df) < min_rows:
        print(f"⚠️ Not enough data to train. Need {min_rows}, got {len(df)}.")
        return None, None, None

    print("\n🧩 DEBUG: Training DataFrame head()")
    print(df.head(5))
    print("Columns:", df.columns.tolist(), "\n")

    lag_cols = [c for c in df.columns if c.startswith("temp_lag") or c.startswith("hum_lag")]
    X_lags = df[lag_cols]
    X_flags = df[["node_1", "node_2"]].astype(int)

    ohe = _make_ohe()
    ohe_mat = ohe.fit_transform(df[["node_name"]].astype("string"))
    ohe_cols = ohe.get_feature_names_out(["node_name"])
    X_ohe = pd.DataFrame(ohe_mat, index=df.index, columns=ohe_cols)

    X = pd.concat([X_lags, X_flags, X_ohe], axis=1)
    y = df["target_next_temp"]

    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X, y)
    return model, ohe, list(X.columns)


# -----------------------------
# PREDICTION
# -----------------------------
def prepare_predict_input(df, n_lags):
    if len(df) < n_lags:
        print(f"⚠️ Not enough rows ({len(df)}) to build {n_lags}-lag prediction input.")
        return None
    feature_dict = {}
    for lag_number in range(1, n_lags + 1):
        feature_dict[f"temp_lag{lag_number}"] = float(df["temperature"].iloc[-lag_number])
        feature_dict[f"hum_lag{lag_number}"] = float(df["humidity"].iloc[-lag_number])
    return pd.DataFrame([feature_dict])


def predict_next_step(model_bundle, df_recent, last_raw_ts, n_lags=3):
    model, ohe, feature_columns = model_bundle
    if model is None:
        return None, None

    X_lags = prepare_predict_input(df_recent, n_lags)
    if X_lags is None:
        return None, None

    last = df_recent.iloc[-1]
    X_flags = pd.DataFrame([{"node_1": int(last.get("node_1", 0)), "node_2": int(last.get("node_2", 0))}])
    X_ohe = pd.DataFrame(
        ohe.transform(pd.DataFrame([{"node_name": str(last.get("node_name", ""))}]).astype("string")),
        columns=ohe.get_feature_names_out(["node_name"])
    )

    X = pd.concat([X_lags, X_flags, X_ohe], axis=1)
    for col in feature_columns:
        if col not in X:
            X[col] = 0.0
    X = X[feature_columns]

    print("\n🧮 DEBUG: Prediction Input DataFrame")
    print(X)
    print("Feature count:", len(X.columns), "\n")

    y_pred = model.predict(X)[0]
    next_ts = last_raw_ts + pd.to_timedelta(config['rf_model']['FREQUENCY'])
    return next_ts, float(y_pred)

# -----------------------------
# TRAINING WINDOW (Cap Data)
# -----------------------------
def take_training_window(df, window_size):
    """
    Keep only the most recent 'window_size' rows for training.
    Prevents training on too much old data.
    """
    if len(df) > window_size:
        start_index = len(df) - window_size
        end_index = len(df)
        df_window = df.iloc[start_index:end_index]
        return df_window
    else:
        return df