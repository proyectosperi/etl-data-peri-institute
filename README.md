# etl-data-peri-institute

Pipeline ETL automatizado para extraer datos de Google Sheets y cargarlos a Supabase.

## 🔄 Funcionalidad

### Ejecución Diaria Automática
- **Horario**: 1:00 AM hora peruana (UTC-5) todos los días
- **Datos procesados**: Día anterior a la fecha de ejecución

### Estrategia de Carga

#### Datos Maestros (UPSERT)
- **Cursos**: Se actualizan o insertan según `codigo_curso`
- **Estudiantes**: Se actualizan o insertan según `codigo_estudiante`

#### Datos Transaccionales (INSERT con filtro de fecha)
- **Matrículas**: Solo se cargan las del día anterior (filtro por `fecha_matricula`)
- **Pagos**: Solo se cargan los del día anterior (filtro por `fecha_pago`)

## 📋 Requisitos

```bash
pip install -r requirements.txt
```

## ⚙️ Configuración

Crear archivo `.env` con las siguientes variables:

```env
# Supabase
SUPABASE_URL=tu_url_supabase
SUPABASE_SERVICE_ROLE_KEY=tu_service_role_key
SUPABASE_KEY=tu_anon_key

# Google Sheets
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account",...}
Matricula_PI_ID=id_de_tu_spreadsheet

# Hojas de cálculo
WORKSHEET_CURSOS=nombre_hoja_cursos
WORKSHEET_MATRICULAS=nombre_hoja_matriculas
WORKSHEET_PAGOS=nombre_hoja_pagos
WORKSHEET_ESTUDIANTES=nombre_hoja_estudiantes
```

## 🚀 Ejecución

### Manual
```bash
cd etl
python pipeline.py
```

### Automática (GitHub Actions)
El archivo `.github/workflows/daily-etl.yml` ejecuta el pipeline diariamente.

#### Configurar Secrets en GitHub
1. Ve a Settings → Secrets and variables → Actions
2. Agrega todos los secrets necesarios (ver sección Configuración)

## 📊 Logs

El pipeline genera logs detallados que incluyen:
- ✓ Confirmación de cargas exitosas
- ✗ Errores específicos por tabla
- Conteo de registros procesados por tabla
- Fecha objetivo de extracción
- Resumen final del ETL

## 📁 Estructura

```
etl/
├── extract.py      # Extracción desde Google Sheets
├── transform.py    # Transformación de datos
├── load.py         # Carga a Supabase
├── pipeline.py     # Orquestación del ETL
├── logger.py       # Configuración de logs
└── output/         # Archivos temporales
```