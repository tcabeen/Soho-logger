import os
from typing import Iterator, Dict, Any
import argparse
import re
import psycopg2
import shutil


def setup():
    #log_directory = '/var/log/soho/'
    #log_file_filter = 'soho.log-\d+'

    parser = argparse.ArgumentParser(description='Pass log file location and regex to determine which logs to load')
    parser.add_argument(
        '-d',
        '--log_directory',
        type = str,
        default = '/var/log/soho/',
        help='a fully-qualified directory name'
    )
    parser.add_argument(
        '-m',
        '--match_regex',
        type = str,
        default = 'soho.log-\d{8}',
        help='a regular expression to match desired files in source directory'
    )

    args = parser.parse_args()

    return args


def file_move(src_file_fq, dest_file_fq, overwrite=False):
    (copy_success, copy_msg) = file_copy(src_file_fq, dest_file_fq, overwrite)
    if not copy_success:
        return False, copy_msg

    (delete_success, delete_msg) = file_delete(src_file_fq)
    if not delete_success:
        return False, delete_msg

    return True, ""


def file_copy(src_file_fq, dest_file_fq, overwrite=False):
    if os.path.isfile(src_file_fq) == False:
        return False, f"{src_file_fq} does not exist"

    if os.path.isfile(dest_file_fq) == True and overwrite == False:
        return False, f"{dest_file_fq} already exists. Set overwrite to True if you wish to force it."

    try:
        function_msg = shutil.copyfile(src_file_fq, dest_file_fq)
    except (OSError, IOError) as e:
        return False, e
    except Exception as e:
        return False, f"Copy failed because {e}"

    return True, function_msg


def file_delete(src_file_fq):
    if os.path.isfile(src_file_fq) == False:
        return False, "That file isn't there"

    try:
        os.remove(src_file_fq)
    except Exception as e:
        return False, f"Delete failed because {e}"
    else:
        return True, ""


def db_ins_raw(cur, vals):
    try:
        sql_query = "INSERT INTO url_log_raw (source_file, line_number, raw_line) VALUES (%s, %s, %s) RETURNING url_log_id"
        cur.execute(sql_query, vals)
    except (Exception, psycopg2.Error) as error:
        print("Record insertion failed")
        return False, None, error
    else:
        count = cur.rowcount
        return True, count, "Success"


def db_sel_ins_cnt(cur, clause):
    sql_query = "SELECT COUNT(*) FROM url_log_raw WHERE source_file = %s"
    where_clause = (clause, )

    try:
        cur.execute(sql_query, where_clause)
        inserted_count = cur.fetchone()[0]
    except Exception as e:
        return False, e, None
    else:
        return True, "Nice.", inserted_count


def db_sel_raw(cur, clause):
    sql_query = "SELECT * FROM url_log_raw WHERE source_file = %s"
    print(f"Selecting {sql_query} using {clause}")
    where_clause = (clause, )
    print(where_clause)

    try:
        cur.execute(sql_query, where_clause)
        inserted_records = cur.fetchall()
    except Exception as e:
        return False, e, None
    else:
        return True, "Nice.", inserted_records


def db_sel_vers(cur):
    try:
        sql_query = 'SELECT version()'
        cur.execute(sql_query)
        version = cur.fetchone()[0]
    except Error as e:
        return False, e, None
    else:
        return True, "", version


def db_get_conn():
    con = None

    try:
        con = psycopg2.connect(
            host = "localhost",
            database = "soho_logs",
            user = "loggy",
            password = "Mighty fine."
        )
    except psycopg2.DatabaseError as e:
        return False, e, None
    else:
        return True, "", con


def main():
    args = setup()
    log_directory = args.log_directory
    match_regex = args.match_regex
    complete_directory = log_directory + 'etl_complete/'

    # Get a list of all the files in /var/log/soho
#    log_file_list = [f.name for f in os.scandir(log_directory) if f.is_file()]
    log_file_list = [f for f in os.listdir(log_directory) if re.match(match_regex, f)]

    if len(log_file_list) > 0:
        log_file_list.sort()
    else:
        print("No log files match your criteria")

    # Get the database connection once for the script
    (con_success, con_msg, db_conn) = db_get_conn()

    if not con_success:
        print("Unable entirely to connect to database.")
        print(f"ERROR: {con_msg}")
        sys.exit(1)

    # Loop through each file
    for this_log_file in log_file_list:
        this_log_file_fq = log_directory + this_log_file
        file_msg = f"Processing {this_log_file_fq}"
        print('-' * len(file_msg))
        print(file_msg)
        print('-' * len(file_msg))

        # Open the file and attempt to parse data on each line
        with open(this_log_file_fq, 'r') as f:
            line_num = 0
            db_cur = db_conn.cursor()

            for line in f:
                line_num += 1

                row_values = (this_log_file, line_num, line)
                #row_values.replace()??
                row_ins_success, row_ins_count, row_ins_msg = db_ins_raw(db_cur, row_values)
                if row_ins_success:
                    db_conn.commit()
                else:
                    print(f"Record insertion failed on line {line_num} --------")
                    print(row_values)
                    print(row_ins_msg)
                    continue

            # Show inserted records
            #(get_success, get_msg, inserted_records) = db_sel_raw(db_cur, this_log_file)

            # Count inserted records
            (ins_success, ins_msg, file_recs_inserted) = db_sel_ins_cnt(db_cur, this_log_file)
            if ins_success:
                print(file_recs_inserted)
            else:
                print(f"ERROR: {ins_msg}")
                continue

            # Compare insert count with inserted count
            if line_num != file_recs_inserted:
                print(f"ERROR: {this_log_file} contains {line_num} records, but url_log_raw has {file_recs_inserted} inserted.")
                continue
            else:
                print("Oh, sweet. Insert went well.")
            
            (move_success, move_msg) = file_move(log_directory+this_log_file, complete_directory+this_log_file)
            if not move_success:
                print(f"ERROR: {move_msg}")
            
            if db_cur:
                db_cur.close()

    # Close db connection
    if db_conn:
        db_conn.close()


if __name__ == "__main__":
    main()
