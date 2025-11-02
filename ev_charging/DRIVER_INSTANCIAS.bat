@echo off
setlocal enabledelayedexpansion
TITLE DESPLIEGUE DRIVERS

echo ================================
echo  ESCALANDO DRIVER A INSTANCIAS
echo ================================
echo.

docker compose up -d driver

echo Esperando 10 segundos para que los contenedores estén listos...
timeout /t 3 /nobreak >nul

echo ================================
echo  EJECUTANDO ev_driver.py EN CADA INSTANCIA
echo ================================
echo.

SET /P NUMERO_INSTANCIAS="Número de instancias : "

SET /P ID_INI="ID inicial : "

for /l %%i in (1,1,%NUMERO_INSTANCIAS%) do (
    set /a ID=%ID_INI% + %%i - 1
    if !ID! LSS 10 (
        set DRIVER_ID=DRIVER_00!ID!
    ) else (
        set DRIVER_ID=DRIVER_0!ID!
    )
    echo Ejecutando Driver %%i con ID !DRIVER_ID!...
    start "Driver !ID!" cmd /k docker exec -it ev_charging-driver-1 python EV_Driver.py kafka:9092 !DRIVER_ID!
)

echo.
echo ✅ Todos los drivers han sido lanzados
echo.
pause
