TITLE ENGINE
SET /P PUERTO_E="PUERTO del ENGINE : "

docker exec -it p1-cp_engine-1 python ev_cp_engine.py kafka:9092 %PUERTO_E%
CMD /k