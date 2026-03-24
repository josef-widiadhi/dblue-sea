@echo off
echo ╔══════════════════════════════════════╗
echo ║     DB Blueprint v2 — starting       ║
echo ╚══════════════════════════════════════╝
echo.
echo Installing dependencies...
pip install -r requirements.txt -q
echo.
echo Pages available:
echo   Dashboard  ^-^> http://localhost:8000/
echo   Blueprint  ^-^> http://localhost:8000/blueprint
echo   Explorer   ^-^> http://localhost:8000/explorer
echo   Profiles   ^-^> http://localhost:8000/profiles
echo   ER Diagram ^-^> http://localhost:8000/diagram
echo.
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
