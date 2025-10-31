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

for /l %%i in (1,1,%NUMERO_INSTANCIAS%) do (
    set /a PUERTO_E=6000 + %%i - 1
    echo Ejecutando CP Monitor %%i en puerto !PUERTO_E! conectado a CP Engine 1...
    start "Monitor %%i" cmd /k docker exec -it ev-charging-cp_monitor-1 python ev_cp_monitor.py cp_engine:!PUERTO_E! central:5000 %%i
)

echo.
echo ✅ Todos los CP Monitor han sido lanzados
echo.
pause
