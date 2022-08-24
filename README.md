# Minecraft Command Usage
A simple script which can automatically retrieve Minecraft server log files from an FTP server, and dumps them into an SQLite database

## Usage
```
usage: command_counter.py [-h] [--db_name DB_NAME] [--use_ftp] [--ftp_host FTP_HOST] [--ftp_port FTP_PORT] [--ftp_user FTP_USER] [--ftp_pass FTP_PASS] [--log_dir LOG_DIR]

options:
  -h, --help           show this help message and exit

Database Configuration:
  --db_name DB_NAME    The name of the database to store command data in

FTP Configuration:
  --use_ftp            Use FTP to download server logs
  --ftp_host FTP_HOST  The hostname of the FTP server to connect to
  --ftp_port FTP_PORT  The port of the FTP server to connect to
  --ftp_user FTP_USER  The username to login to the FTP server with
  --ftp_pass FTP_PASS  The password to login to the FPT server with
  --log_dir LOG_DIR    The local directory to store log files in
```