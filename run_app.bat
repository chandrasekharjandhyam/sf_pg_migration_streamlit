@echo off
echo Starting Snowflake to PostgreSQL Migration Tool...
echo.
echo The application will be available at: http://localhost:8501
echo Press Ctrl+C to stop the application
echo.

cd /d "%~dp0"
C:/Python312/python.exe -m streamlit run app.py --server.port=8501 --server.headless=true --browser.gatherUsageStats=false

pause
