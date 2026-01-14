import time
import pandas as pd
from helpers.db_connection import pool
from model.ml_functions import lgbm
import configparser

config = configparser.ConfigParser()
config.read("server/setting.conf")


def worker_loop(conn):
    made = 0
    idle = config.getfloat("lgbm_model", "SLEEP_IDLE")
    FREQ = config["lgbm_model"]["FREQUENCY"]
    n_lags = 23

    # ---------- Initial training ----------
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT timestamp, temperature, humidity, node_name, site_name, isactive
        FROM vw_node_site_readings
        ORDER BY TIMESTAMP DESC; 
    """
    )  # isactive = 1 is already filtered in the vw_node_site_readings view
    rows = cur.fetchall()
    cur.close()

    # df_all = pd.DataFrame(rows)
    # # df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
    # df_all = df_all.set_index("timestamp")

    df_clean = lgbm.clean_dataframe(rows, 0.1)
    df_lagged = (
        df_clean.groupby(["node_name", "site_name"], group_keys=False)
        .apply(lambda g: lgbm.enforce_fixed_interval(g, FREQ))
        .groupby(["node_name", "site_name"], group_keys=False)
        .apply(lambda g: lgbm.make_lag_features(g, n_lags=n_lags))
    )
    df_time = lgbm.add_time_features(df_lagged)
    df_cap = lgbm.take_training_window(
        df_time, config.getint("lgbm_model", "MIN_REQUIRED_TRAINSET")
    )

    print("\nDEBUG: DataFrame before training")
    print(df_cap.head(10))
    print("Shape:", df_cap.shape, "\n")

    print("Model: LightGBM: Train sets - ", len(df_cap))
    model_bundle = lgbm.train_model(df_cap)

    if model_bundle[0] is not None:
        print("[Model trained successfully!]")
    else:
        print("[Model training failed!]")

    # ---------- Loop ----------
    while True:
        job = lgbm.claim_job(conn)  #
        if not job:
            print("No job found, sleeping...")
            time.sleep(idle)
            idle = min(2.0, idle * 1.5)
            continue

        idle = config.getfloat("lgbm_model", "SLEEP_IDLE")

        node_id = job["node_id"]
        site_id = job["site_id"]
        ts_for_insert_and_queue = pd.to_datetime(job["ts"])
        print(
            f"\n---Processing job from node '{node_id}' and site '{site_id}' at {ts_for_insert_and_queue}"
        )

        latest_rows = lgbm.fetch_rows_upto(node_id, site_id, ts_for_insert_and_queue)  #
        df_latest = lgbm.clean_dataframe(
            latest_rows, 0.5
        )  # 250 latest rows of specific node_id and site_id
        df_latest = lgbm.enforce_fixed_interval(df_latest, FREQ)

        if len(df_latest) < max(
            config.getint("lgbm_model", "MIN_REQUIRED_ROWS"), n_lags
        ):
            print(
                f"***Node '{node_id}' at site '{site_id}' has insufficient data ({len(df_latest)} rows). Skipping prediction."
            )
            lgbm.job_fail(
                conn,
                node_id,
                site_id,
                ts_for_insert_and_queue,
                reason="Insufficient data for prediction",
            )  #
            continue
        df_time = lgbm.add_time_features(df_latest)
        last_raw_ts = ts_for_insert_and_queue
        predict_ts, predict_value = lgbm.predict_next_step(
            model_bundle=model_bundle,  # trained model
            df_recent=df_time,  # input data for prediction
            last_raw_ts=last_raw_ts,  # timestamp of last raw reading
            n_lags=n_lags,  # number of lag features for input to match training set
        )

        if predict_value is not None:
            site_id = lgbm.get_site_id_for_node(conn, node_id, ts_for_insert_and_queue)
            if site_id is None:
                print(
                    f"Failed to resolve site_id for node '{node_id}'. Marking job failed."
                )
                lgbm.job_fail(
                    conn,
                    node_id,
                    site_id,
                    ts_for_insert_and_queue,
                    reason="Missing site_id for node",
                )  #
                continue

            print(
                f"---Predicted {predict_value:.2f}°C for site_id {site_id} and node_id {node_id} at {predict_ts}"
            )

            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO tbl_predicted_and_timestamp
                        (node_id, site_id, predicted_timestamp, predicted_temperature)
                    VALUES (%s, %s, %s, %s)
                """,
                    (node_id, site_id, predict_ts, predict_value),
                )
                conn.commit()
                cur.close()
                print("[Prediction saved to database!]")
            except Exception as e:
                print("Failed to save prediction:", e)
                lgbm.job_fail(
                    conn,
                    node_id,
                    site_id,
                    ts_for_insert_and_queue,
                    reason=f"DB insert failed: {e}",
                )  #
                continue

            lgbm.job_success(conn, node_id, site_id, ts_for_insert_and_queue)  #
            made += 1
            print(f"Retrain Count {config['lgbm_model']['RETRAIN_AFTER']}: {made}")

            if made >= config.getint("lgbm_model", "RETRAIN_AFTER"):
                print("[Retraining model with latest data...]")
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    """
                    SELECT timestamp, temperature, humidity, node_name, site_name, isactive
                    FROM vw_node_site_readings
                    ORDER BY timestamp DESC;
                """
                )
                rows = cur.fetchall()
                cur.close()

                # df_all = pd.DataFrame(rows)
                # df_all["timestamp"] = pd.to_datetime(df_all["timestamp"])
                # df_all = df_all.set_index("timestamp")

                df_clean = lgbm.clean_dataframe(rows, 0.5)
                df_lagged = (
                    df_clean.groupby(["node_name", "site_name"], group_keys=False)
                    .apply(lambda g: lgbm.enforce_fixed_interval(g, FREQ))
                    .groupby(["node_name", "site_name"], group_keys=False)
                    .apply(lambda g: lgbm.make_lag_features(g, n_lags=n_lags))
                )
                df_time = lgbm.add_time_features(df_lagged)
                df_cap = lgbm.take_training_window(
                    df_time, config.getint("lgbm_model", "MIN_REQUIRED_TRAINSET")
                )

                print("\nDEBUG: DataFrame before retraining")
                print(df_cap.head(10))
                print("Shape:", df_cap.shape, "\n")

                model_bundle = lgbm.train_model(df_cap)
                if model_bundle[0] is not None:
                    print("[Model retrained successfully!]")
                    made = 0
        else:
            print("[Prediction failed!]")
            lgbm.job_fail(
                conn,
                node_id,
                site_id,
                ts_for_insert_and_queue,
                reason="Prediction step failed",
            )


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
