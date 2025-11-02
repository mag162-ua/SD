@echo off
setlocal enabledelayedexpansion
TITLE DESPLIEGUE CP MONITOR

echo ================================
echo  ESCALANDO CP MONITOR A INSTANCIAS
echo ================================
echo.

docker compose up -d cp_monitor

echo Esperando 10 segundos para que los contenedores estén listos...
timeout /t 10 /nobreak >nul

echo ================================
echo  EJECUTANDO ev_cp_monitor.py EN CADA INSTANCIA
echo ================================
echo.

SET /P NUMERO_INSTANCIAS="Número de instancias : "

SET /P ID_INI="ID inicial : "

SET /P PUERTO_INI="Puerto inicial : "

for /l %%i in (1,1,%NUMERO_INSTANCIAS%) do (
    set /a PUERTO_E=%PUERTO_INI% + %%i - 2 + %ID_INI%
    set /a ID=%ID_INI% + %%i - 1
    echo Ejecutando CP Monitor %%i en puerto !PUERTO_E! conectado a CP Engine 1...
    start "Monitor %%i" cmd /k docker exec -it ev_charging-cp_monitor-1 python EV_CP_M.py cp_engine:!PUERTO_E! central:5000 !ID!
)

echo.
echo ✅ Todos los CP Monitor han sido lanzados
echo.
pause
