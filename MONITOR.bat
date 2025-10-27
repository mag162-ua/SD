TITLE MONITOR
SET /P NUMERO_DADO ="ID del CP : "
SET /P IP_PUERTO_E="IP:PUERTO del ENGINE : "
SET /P IP_PUERTO_C="IP_PUERTO de la CENTRAL: "
docker exec -it p1-cp_monitor-1 python ev_cp_monitor.py %IP_PUERTO_E% %IP_PUERTO_C% %NUMERO_DADO% 
CMD /k