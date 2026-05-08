#!/bin/bash
set -e

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
SERVER=mssql

echo ">>> Waiting for SQL Server to be ready..."
until $SQLCMD -S "$SERVER" -U sa -P "$SA_PASSWORD" -C -Q "SELECT 1" &>/dev/null; do
    sleep 2
done
echo ">>> SQL Server is up."

echo ">>> Creating database '$SQL_SERVER_DB' if not exists..."
$SQLCMD -S "$SERVER" -U sa -P "$SA_PASSWORD" -C -Q \
    "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '${SQL_SERVER_DB}') CREATE DATABASE [${SQL_SERVER_DB}]"

echo ">>> Running noaa_dw_drop.sql..."
$SQLCMD -S "$SERVER" -U sa -P "$SA_PASSWORD" -C -I -d "$SQL_SERVER_DB" -i /scripts/noaa_dw_drop.sql

echo ">>> Running noaa_dw_schema.sql..."
$SQLCMD -S "$SERVER" -U sa -P "$SA_PASSWORD" -C -I -d "$SQL_SERVER_DB" -i /scripts/noaa_dw_schema.sql

echo ">>> Schema initialized successfully."
