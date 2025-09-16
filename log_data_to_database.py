import argparse
import multiprocessing
import os
import json
import re
import sys
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import db_laddms

from dotenv import load_dotenv
load_dotenv()


def single_log_file_to_db(log_input_filename: str, intersection_id: int, dry_run: bool = False):
    frame_count = 0
    detection_count = 0
    with open(log_input_filename, "r") as infile:
        if dry_run is False:
            sql_connection = db_laddms.make_sql_connection()
            sql_cursor = db_laddms.make_sql_cursor(sql_connection)
        try:
            first_line = True
            for line in infile:
                if "{" not in line:
                    continue
                try:
                    json_part = line[line.index("{"):]
                    data = json.loads(json_part)
                    for entry in data.get("object_list", []):
                        if first_line is True:
                            this_frame_timestamp = entry["timestamp"]
                            ts_utc = datetime.fromtimestamp(this_frame_timestamp / 1000000, tz=ZoneInfo("UTC"))
                            timestamp_tz = ts_utc.astimezone(tz=ZoneInfo("US/Central"))
                            print(f"fn={log_input_filename}\t\tstart_time={timestamp_tz.isoformat()}")
                            first_line = False
                        if dry_run is True:
                            return f"Dry run -- fn={log_input_filename}"
                        num_detect, t_insert = db_laddms.insert_object_detections(
                            intersection_id=intersection_id,
                            timestamp_tz=None,
                            json_data=None,
                            device_id="log2db",
                            use_db_conn=None,
                            use_db_cursor=sql_cursor,
                            dict_with_object_list=entry,
                        )
                        frame_count += 1
                        detection_count += num_detect
                except Exception as e:
                    traceback.print_exc()
                    print(f"Skipping line due to error: {e}")
                    continue

        except Exception as e:
            traceback.print_exc()
            print(f"Skipping file due to error: {e}")
        finally:
            try:
                sql_cursor.close()
            except:
                pass
    return f"fn={log_input_filename}, num_frames={frame_count}, num_detections={detection_count}"


def result_callback(result):
    print(f"Received result from process.\n\t{result}")


def main(filename_intersection_tuples):
    pool = multiprocessing.Pool(processes=4)
    async_result = pool.starmap_async(single_log_file_to_db, filename_intersection_tuples, callback=result_callback)
    final_results = async_result.get()
    pool.close()
    pool.join()
    print("\n\n", "=" * 40)
    print(f"\nPROCESS POOL COMPLETE\n")
    print("=" * 40, "\n\n")
    with open("log2db_processed.txt", "a") as f:
        for fn, int_id, dry_run_mode in filename_intersection_tuples:
            if dry_run_mode is False:
                f.write(f"{int_id},{fn}\n")



if __name__ == "__main__":
    # Set up command-line arguments
    parser = argparse.ArgumentParser(description="Insert Ouster log JSON entries to database.")
    parser.add_argument("-i", "--input", required=False, help="Input log file (.log, .log.1, .log.2, ...)")
    parser.add_argument("-l", "--inlist", required=False,
                        help="File (.txt) containing a list of log files to process in parallel.")
    parser.add_argument("-f", "--format", required=False, help="File with formatter {} denoting log number.")
    parser.add_argument("-s", "--start", required=False, type=int, help="Start index (inclusive) of log file to process.")
    parser.add_argument("-e", "--end", required=False, type=int, help="End index (inclusive) of log file to process.")
    parser.add_argument("-d", "--dry", required=False, action="store_true", default=False, help="Dry run mode.")

    args = parser.parse_args()

    list_of_unvalidated_filenames = []
    if args.inlist is not None:
        list_of_unvalidated_filenames = [fn for fn in args.inlist]
    elif args.format is not None:
        if args.start is not None and args.end is not None:
            if args.start < args.end:
                i1, i2 = args.start, args.end
            else:
                i2, i1 = args.start, args.end
            list_of_unvalidated_filenames = [args.format.format(i) for i in range(i1, i2 + 1)]
            print(f"Building log files from {i1} to {i2}.")
        else:
            raise ValueError("Specified file formatter but no start/end log indexes.")
    else:
        list_of_unvalidated_filenames = [args.input]

    # Mapping from folder name to intersection DID
    folder_to_did = {
        "5th_Broadway": 11,
        "4th_Broadway": 12,
        "3rd_Broadway": 13,
        "2nd_Broadway": 14,
        "1st_Broadway": 15,
    }

    list_of_validated_filenames = []
    for fn in list_of_unvalidated_filenames:
        # Validate input file extension
        if not re.match(r".*\.log(\.\d+)?$", fn):
            print("Error: Input file must end in .log, .log.1, .log.2, etc.")
            continue

        # Walk up the directory structure to find a matching folder
        input_parts = os.path.abspath(fn).split(os.sep)
        intersection_folder = None
        for part in reversed(input_parts[:-1]):  # skip the actual filename
            if part in folder_to_did:
                intersection_folder = part
                break
        if intersection_folder is None:
            print(f"Error: No known intersection folder found in path '{fn}'")
            sys.exit(1)

        list_of_validated_filenames.append((fn, folder_to_did[intersection_folder], args.dry))

    print(f"Executing log to DB on {len(list_of_validated_filenames)} files.")
    main(list_of_validated_filenames)
