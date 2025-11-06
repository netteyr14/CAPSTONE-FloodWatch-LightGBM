import time
import pandas as pd
from helpers.db_connection import pool
from model.ml_functions import rf
import configparser

config = configparser.ConfigParser()
config.read('server/setting.conf')


def worker_loop(conn):
    made = 0
    idle = config.getfloat('rf_model', 'SLEEP_IDLE')
    n_lags = 3

    # ---------- Initial training ----------
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT timestamp, temperature, humidity, node_1, node_2, node_name
        FROM dht11_random_forest
        ORDER BY timestamp DESC;
    """)
    rows = cur.fetchall()
    cur.close()

    df_all = pd.DataFrame(rows)
    df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
    df_all = df_all.set_index("timestamp")

    df_clean = rf.clean_dataframe(df_all)
    df_fixed = rf.enforce_fixed_interval(df_clean, config['rf_model']['FREQUENCY'])
    df_lagged = rf.make_lag_features(df_fixed, n_lags=n_lags)
    df_cap = rf.take_training_window(df_lagged, config.getint('rf_model', 'MIN_REQUIRED_TRAINSET'))

    print("\n🧠 DEBUG: DataFrame before training")
    print(df_cap.head(10))
    print("Shape:", df_cap.shape, "\n")

    print("Model: RANDOM FOREST: Train sets - ", len(df_cap))
    model_bundle = rf.train_model(df_cap)

    if model_bundle[0] is not None:
        print("[Model trained successfully!]")
    else:
        print("[Model training failed!]")

    # ---------- Loop ----------
    while True:
        job = rf.claim_job(conn)
        if not job:
            print("No job found, sleeping...")
            time.sleep(idle)
            idle = min(2.0, idle * 1.5)
            continue

        idle = config.getfloat('rf_model', 'SLEEP_IDLE')
        node = job["node_name"]
        ts_for_insert_and_queue = pd.to_datetime(job["ts"])
        print(f"\n---Processing job from node '{node}' at {ts_for_insert_and_queue}")

        latest_rows = rf.fetch_rows_upto(node, ts_for_insert_and_queue)
        df_latest = rf.clean_dataframe(latest_rows)

        if len(df_latest) < config.getint('rf_model', 'MIN_REQUIRED_ROWS') or len(df_latest) < n_lags:
            print(f"***Node '{node}' has insufficient data ({len(df_latest)} rows). Skipping prediction.")
            rf.job_fail(conn, node, ts_for_insert_and_queue, reason="Insufficient data for prediction")
            continue

        df_latest = rf.enforce_fixed_interval(df_latest, config['rf_model']['FREQUENCY'])
        print("\n🧩 DEBUG: DataFrame before prediction")
        print(df_latest.tail(10))
        print("Shape:", df_latest.shape, "\n")

        last_raw_ts = ts_for_insert_and_queue
        predict_ts, predict_value = rf.predict_next_step(
            model_bundle=model_bundle,
            df_recent=df_latest,
            last_raw_ts=last_raw_ts,
            n_lags=n_lags
        )

        if predict_value is not None:
            print(f"---Predicted {predict_value:.2f}°C for {predict_ts}")

            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO tbl_predicted_and_timestamp (node_name, predicted_timestamp, predicted_temperature)
                    VALUES (%s, %s, %s)
                """, (node, predict_ts, predict_value))
                conn.commit()
                cur.close()
                print("[Prediction saved to database!]")
            except Exception as e:
                print("Failed to save prediction:", e)

            rf.job_success(conn, node, ts_for_insert_and_queue)
            made += 1
            print(f"Retrain Count{config['rf_model']['RETRAIN_AFTER']}: {made}")

            if made >= config.getint('rf_model', 'RETRAIN_AFTER'):
                print("[Retraining model with latest data...]")
                cur = conn.cursor(dictionary=True)
                cur.execute("""
                    SELECT timestamp, temperature, humidity, node_1, node_2, node_name
                    FROM dht11_random_forest
                    ORDER BY timestamp DESC;
                """)
                rows = cur.fetchall()
                cur.close()

                df_all = pd.DataFrame(rows)
                df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
                df_all = df_all.set_index("timestamp")

                df_clean = rf.clean_dataframe(df_all)
                df_fixed = rf.enforce_fixed_interval(df_clean, config['rf_model']['FREQUENCY'])
                df_lagged = rf.make_lag_features(df_fixed, n_lags=n_lags)
                df_cap = rf.take_training_window(df_lagged, config.getint('rf_model', 'MIN_REQUIRED_TRAINSET'))

                print("\n🧠 DEBUG: DataFrame before retraining")
                print(df_cap.head(10))
                print("Shape:", df_cap.shape, "\n")

                model_bundle = rf.train_model(df_cap)
                if model_bundle[0] is not None:
                    print("[Model retrained successfully!]")
                    made = 0
        else:
            print("[Prediction failed!]")
            rf.job_fail(conn, node, ts_for_insert_and_queue, reason="Prediction step failed")


if __name__ == "__main__":
    try:
        conn = pool.get_connection()
        worker_loop(conn)
    except Exception as e:
        print("Error:", e)
    finally:
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass
