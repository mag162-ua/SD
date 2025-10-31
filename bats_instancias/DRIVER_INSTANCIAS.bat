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

for /l %%i in (1,1,2) do (
    if %%i LSS 10 (
        set DRIVER_ID=DRIVER_00%%i
    ) else (
        set DRIVER_ID=DRIVER_0%%i
    )
    echo Ejecutando Driver %%i con ID !DRIVER_ID!...
    start "Driver %%i" cmd /k docker exec -it ev-charging-driver-1 python ev_driver.py kafka:9092 !DRIVER_ID!
)

echo.
echo ✅ Todos los drivers han sido lanzados
echo.
pause
