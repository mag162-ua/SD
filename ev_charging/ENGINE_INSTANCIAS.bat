@echo off
setlocal enabledelayedexpansion
TITLE DESPLIEGUE CP ENGINE

echo ================================
echo  ESCALANDO CP ENGINE A INSTANCIAS
echo ================================
echo.

docker compose up -d cp_engine

echo Esperando 10 segundos para que los contenedores estén listos...
timeout /t 10 /nobreak >nul

echo ================================
echo  EJECUTANDO ev_cp_engine.py EN CADA INSTANCIA
echo ================================
echo.

SET /P NUMERO_INSTANCIAS="Número de instancias : "

SET /P PUERTO_INI="Puerto inicial : "

for /l %%i in (1,1,%NUMERO_INSTANCIAS%) do (
    set /a PUERTO_E=%PUERTO_INI% + %%i - 1
    echo Ejecutando CP Engine %%i en puerto !PUERTO_E!...
    start "Engine %%i" cmd /k docker exec -it ev_charging-cp_engine-1 python EV_CP_E.py kafka:9092 !PUERTO_E!
)

echo.
echo ✅ Todos los CP Engine han sido lanzados
echo.
pause

