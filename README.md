# Sistema de Inteligencia Financiera

## Instalación

### Base de Datos

```bash
psql -U postgres
CREATE DATABASE uif_analisis;
\q

psql -U postgres -d uif_analisis -f database/schema.sql
```

### Python

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Configuración

```bash
cp .env.example .env
```

Editar `.env` con credenciales correctas.

## Ejecución

```bash
cd app
streamlit run main.py
```

## Estructura

```
/database
  schema.sql          - DDL completo
/app
  database.py         - Conexión DB
  etl.py             - Carga y limpieza RO
  casos.py           - Gestión de casos
  analisis.py        - Análisis transaccional
  tipologias.py      - Motor de tipologías
  redes.py           - Análisis de redes
  reportes.py        - Generación de reportes
  main.py            - Aplicación Streamlit
```

## Flujo de Trabajo

1. Cargar archivo Excel RO
2. Crear caso
3. Seleccionar personas
4. Ejecutar análisis
5. Detectar tipologías
6. Analizar red
7. Generar reportes
