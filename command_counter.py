from io import TextIOWrapper
from os import scandir, path
from argparse import ArgumentParser, Namespace
from tempfile import mkdtemp
import gzip as gz
from socket import gaierror
from ftplib import FTP
import sqlite3 as sql
from datetime import datetime
import re as regex
from tokenize import group

cheat_commands = {"creative", "spectator",
                  "survival", "give", "xp", "tp", "speed", "time"}

custom_captures = {
    "gamemode": "gamemode\s(survival|creative|adventure|spectator)",
    "gm": "gm\s(survival|creative|adventure|spectator)",
    "kit": "kit\s(commandbook|rulebook)"
}

aliases = {
    "gamemode creative": "creative",
    "gamemode survival": "survival",
    "gamemode adventure": "adventure",
    "gamemode spectator": "spectator",
    "gm creative": "creative",
    "gm survival": "survival",
    "gm adventure": "adventure",
    "gm spectator": "spectator",
    "kit commandbook": "commandbook",
    "kit rulebook": "rulebook"
}

logfile_pattern = regex.compile(
    "[0-9][0-9][0-9][0-9]\-[0-9][0-9]\-[0-9][0-9]\-[0-9]+\.log\.gz"
)
log_pattern = regex.compile(
    "\[([0-9][0-9]):([0-9][0-9]):([0-9][0-9])\]\s\[Server\sthread\/INFO\]\:\s([A-Za-z]+)\sissued\sserver\scommand\:\s\/([A-Za-z]+)"
)
filename_pattern = regex.compile(
    "([0-9][0-9][0-9][0-9])\-([0-9][0-9])\-([0-9][0-9])(.*)"
)

arguments: Namespace = None

database_connection: sql.Connection = None


def download_logs():
    print("Attempting to download logs over FTP")

    server_host = arguments.ftp_host
    server_port = int(arguments.ftp_port)
    server_user = arguments.ftp_user
    server_pass = arguments.ftp_pass
    log_directory = arguments.log_dir

    ftp = FTP()
    ftp.connect(host=server_host, port=server_port)
    ftp.login(user=server_user, passwd=server_pass)

    ftp.cwd(log_directory)
    file_list = []
    ftp.retrlines("NLST", callback=lambda line: file_list.append(line))

    temp_directory = mkdtemp()
    logs_downloaded = 0

    for file_name in file_list:
        match = regex.match(logfile_pattern, file_name)
        if not match:
            continue

        local_path = f"logs/{match.group(0)[0:-3]}"
        if path.exists(local_path):
            continue

        compressed_file_path = temp_directory + match.group(0)
        with open(compressed_file_path, "wb") as file:
            return_command = f"RETR {match.group(0)}"
            print(f"Downloading {match.group(0)}...")
            ftp.retrbinary(return_command, file.write)

        with gz.open(compressed_file_path, "rb") as compressed_file:
            with open(local_path, "wb") as decompressed_file:
                data = compressed_file.read()
                decompressed_file.write(data)

        logs_downloaded += 1

    print(f"Downloaded {logs_downloaded} logs from {server_host}")


def setup_database():
    global database_connection
    database_connection = sql.connect(arguments.db_name)

    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS COMMAND_USAGE(
        TIME DATETIME NOT NULL, 
        PLAYER VARCHAR NOT NULL,
        COMMAND VARCHAR NOT NULL,
        PRIMARY KEY(TIME, PLAYER)
    );    
    """)
    database_connection.execute(
        """CREATE TABLE IF NOT EXISTS LOG_FILES(
        ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        FILENAME VARCHAR NOT NULL
    );
    """
    )


def get_file_list() -> list[str]:
    all_files = scandir("./logs")
    log_files = []
    for file in all_files:
        if not file.is_file:
            continue

        if not file.path.endswith(".log"):
            continue

        log_files.append(file.path)

    return log_files


def process_file(file_handle: TextIOWrapper, file_match: regex.Match, file_date: datetime):
    print(f"Processing file {file_match.group(0)}")

    for line in file_handle:
        regex_match = regex.match(log_pattern, line)
        if regex_match:
            command_time = datetime(file_date.year, file_date.month, file_date.day, int(
                regex_match.group(1)), int(regex_match.group(2)), int(regex_match.group(3)))

            player = regex_match.group(4)
            command = regex_match.group(5).lower()

            if command in custom_captures:
                command_match = regex.search(custom_captures[command], line)
                if not command_match:
                    continue
                else:
                    command = command_match.group(0)

            if command in aliases:
                command = aliases[command]

            try:
                query = f"INSERT INTO COMMAND_USAGE VALUES(datetime({int(command_time.timestamp())}, 'unixepoch'), \"{player}\", \"{command}\");"
                database_connection.execute(query)
            except sql.IntegrityError:
                print(
                    f"Unique constraint failed ({player}, {command_time}, {command})")

    database_connection.execute(
        f"INSERT INTO LOG_FILES(FILENAME) VALUES(\"{file_match.group(0)}\")")
    database_connection.commit()


def parse_arguments():
    parser = ArgumentParser()

    db_group = parser.add_argument_group("Database Configuration")
    db_group.add_argument(
        "--db_name", help="The name of the database to store command data in", default="command_usage.db")

    ftp_group = parser.add_argument_group("FTP Configuration")
    ftp_group.add_argument("--use_ftp", action="store_true",
                           help="Use FTP to download server logs")
    ftp_group.add_argument(
        "--ftp_host", help="The hostname of the FTP server to connect to", default="localhost")
    ftp_group.add_argument("--ftp_port", type=int,
                           help="The port of the FTP server to connect to", default=21)
    ftp_group.add_argument(
        "--ftp_user", help="The username to login to the FTP server with", default="anonymous")
    ftp_group.add_argument(
        "--ftp_pass", help="The password to login to the FPT server with", default="")
    ftp_group.add_argument(
        "--log_dir", help="The local directory to store log files in", default="logs")

    global arguments
    arguments = parser.parse_args()


def main():
    parse_arguments()
    setup_database()
    if arguments.use_ftp:
        download_logs()

    for log_file in get_file_list():

        file_match = regex.search(filename_pattern, log_file)
        if not file_match:
            continue

        query = f"SELECT * FROM LOG_FILES WHERE FILENAME=\"{file_match.group(0)}\""
        cursor = database_connection.execute(query)
        if len(cursor.fetchall()) > 0:
            print(f"{file_match.group(0)} has already been processed")
            continue

        file_date = datetime(int(file_match.group(1)), int(
            file_match.group(2)), int(file_match.group(3)))

        with open(log_file, "r") as file_handle:
            process_file(file_handle, file_match, file_date)


if __name__ == "__main__":
    main()
