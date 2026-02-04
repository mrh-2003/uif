#!/bin/bash

DB_NAME="uif"
DB_USER="postgres"

echo "Creando base de datos..."
psql -U $DB_USER -c "DROP DATABASE IF EXISTS $DB_NAME;"
psql -U $DB_USER -c "CREATE DATABASE $DB_NAME;"

echo "Ejecutando schema..."
psql -U $DB_USER -d $DB_NAME -f database/schema.sql

echo "Base de datos inicializada correctamente"
