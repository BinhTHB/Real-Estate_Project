@echo off
REM MinIO Server Startup Script for Windows

echo 🚀 Starting MinIO Server...

REM Create data directory if it doesn't exist
if not exist "data" mkdir data

REM Start MinIO server
REM Note: This assumes MinIO is installed via Chocolatey or binary is in PATH
REM If not installed, download from: https://min.io/download
minio server ./data --console-address ":9001"

echo MinIO server stopped.
pause