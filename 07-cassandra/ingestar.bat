@echo off
setlocal enabledelayedexpansion
echo Iniciando ingesta de Cassandra (M12)...

:: Resolver rutas absolutas
set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%.."
set "DATASET=%REPO_ROOT%\data\lecturas_sensores.csv"
set "SCHEMA=%SCRIPT_DIR%schema.cql"
set "LOAD=%SCRIPT_DIR%load.cql"
set "COMPOSE_FILE=%REPO_ROOT%\docker-compose.yml"

:: Validar que exista el CSV
if not exist "%DATASET%" (
    echo Error: No se encontro el dataset en %DATASET%
    exit /b 1
)

echo Levantando contenedor Cassandra...
docker compose -f "%COMPOSE_FILE%" up -d cassandra

echo Esperando que Cassandra este listo (maximo 2 minutos)...
set /a attempts=0
set /a max_attempts=60

:healthcheck
docker exec nexo-cassandra cqlsh -e "DESCRIBE KEYSPACES;" >nul 2>&1
if %errorlevel% equ 0 goto ready

set /a attempts+=1
if !attempts! geq %max_attempts% (
    echo Error: Cassandra no respondio despues de %max_attempts% intentos.
    echo Revisa si el contenedor se cayo: docker logs nexo-cassandra
    exit /b 1
)
timeout /t 2 >nul
goto healthcheck

:ready
echo Cassandra respondio correctamente.

echo Copiando CSV al contenedor...
docker cp "%DATASET%" nexo-cassandra:/tmp/lecturas_sensores.csv

echo Creando esquema...
docker exec -i nexo-cassandra cqlsh < "%SCHEMA%"

echo Cargando datos...
docker exec -i nexo-cassandra cqlsh < "%LOAD%"

echo Ingesta finalizada correctamente.
endlocal 