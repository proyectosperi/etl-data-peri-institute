import os
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

from logger import get_logger

from extract import (
    extract_sheet_pi_1,
    extract_sheet_pi_2,
    extract_sheet_pi_3,
)
from transform import (
    transform_cursos,
    transform_matriculas,
    transform_pagos_primera_cuota,
    transform_regular_pagos,
    transform_estudiantes,
)
from load import load

logger = get_logger("PIPELINE")

load_dotenv()


def run_pipeline(year=None, month=None):
    """ETL diario que extrae datos del día anterior para matrículas y pagos.
    
    Ejecuta upsert para cursos y estudiantes (datos maestros).
    Ejecuta insert para matrículas y pagos (filtrados por fecha del día anterior).
    """

    # periodo objetivo: ayer
    today = date.today()
    yesterday = today - timedelta(days=1)
    target_date = yesterday.strftime("%Y-%m-%d")

    logger.info(f"===== ETL DIARIO | Fecha objetivo: {target_date} =====")

    # inicializar dfs
    df_cursos_pi_final = pd.DataFrame()
    df_matriculas_pi_final = pd.DataFrame()
    df_primera_cuota_pi_final = pd.DataFrame()
    df_regular_pagos_final = pd.DataFrame()
    df_es_final = pd.DataFrame()

    # ==================== PASO 1: UPSERTS DE DATOS MAESTROS ====================
    logger.info("="*60)
    logger.info("PASO 1: PROCESANDO DATOS MAESTROS (UPSERT)")
    logger.info("="*60)
    
    # ==================== CURSOS (UPSERT) ====================
    logger.info("Procesando hoja Cursos (datos maestros - UPSERT)")
    df_cursos_pi_raw = extract_sheet_pi_1(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_CURSOS"),
        None,
        None,
    )
    if not df_cursos_pi_raw.empty:
        df_cursos_pi_final = transform_cursos(df_cursos_pi_raw)
        logger.info(f"Cursos extraídos y transformados: {len(df_cursos_pi_final)}")
    else:
        logger.warning("No se extrajeron cursos")
    
    # Cargar cursos con upsert
    if not df_cursos_pi_final.empty:
        logger.info(f"Cargando {len(df_cursos_pi_final)} cursos con UPSERT...")
        try:
            load("cursos", df_cursos_pi_final, upsert=True, pk_column="codigo_curso")
            logger.info(f"✓ Cursos cargados exitosamente: {len(df_cursos_pi_final)} registros")
        except Exception as e:
            logger.error(f"✗ Error al cargar cursos: {e}")
            raise
    else:
        logger.info("No hay cursos para cargar")

    # ==================== ESTUDIANTES (UPSERT) ====================
    logger.info("Procesando hoja Estudiantes (datos maestros - UPSERT)")
    df_alumnos_pi_raw = extract_sheet_pi_1(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_ESTUDIANTES"),
        None,
        None,
    )
    if not df_alumnos_pi_raw.empty:
        df_es_final = transform_estudiantes(df_alumnos_pi_raw)
        logger.info(f"Estudiantes extraídos y transformados: {len(df_es_final)}")
    else:
        logger.warning("No se extrajeron estudiantes")
    
    # Cargar estudiantes con upsert
    if not df_es_final.empty:
        logger.info(f"Cargando {len(df_es_final)} estudiantes con UPSERT...")
        try:
            load("estudiantes", df_es_final, upsert=True, pk_column="codigo_estudiante")
            logger.info(f"✓ Estudiantes cargados exitosamente: {len(df_es_final)} registros")
        except Exception as e:
            logger.error(f"✗ Error al cargar estudiantes: {e}")
            raise
    else:
        logger.info("No hay estudiantes para cargar")

    # ==================== PASO 2: INSERTS DE DATOS TRANSACCIONALES ====================
    logger.info("="*60)
    logger.info(f"PASO 2: PROCESANDO DATOS TRANSACCIONALES - Fecha: {target_date}")
    logger.info("="*60)
    
    # ==================== MATRICULAS (FILTRADAS POR FECHA) ====================
    logger.info(f"Procesando hoja Matriculas (filtro: {target_date})")
    df_matriculas_pi_raw = extract_sheet_pi_2(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_MATRICULAS"),
        None,
        None,
    )
    if not df_matriculas_pi_raw.empty:
        # Filtrar en RAW por Marca temporal antes de transformar
        if "Marca temporal" in df_matriculas_pi_raw.columns:
            df_matriculas_pi_raw["Marca temporal"] = pd.to_datetime(df_matriculas_pi_raw["Marca temporal"], dayfirst=True, errors="coerce")
            total_before = len(df_matriculas_pi_raw)
            df_matriculas_pi_raw["_fecha_marca"] = df_matriculas_pi_raw["Marca temporal"].dt.strftime("%Y-%m-%d")
            df_matriculas_pi_raw = df_matriculas_pi_raw.loc[
                df_matriculas_pi_raw["_fecha_marca"] == target_date
            ].reset_index(drop=True)
            logger.info(f"Matrículas RAW filtradas por Marca temporal: {len(df_matriculas_pi_raw)} de {total_before} (fecha={target_date})")
        else:
            logger.warning("Columna 'Marca temporal' no encontrada en matrículas, no se aplicó filtro de fecha")
        
        df_matriculas_pi_final = transform_matriculas(df_matriculas_pi_raw)
        df_primera_cuota_pi_final = transform_pagos_primera_cuota(df_matriculas_pi_raw)
        
        # Filtrar pagos de primera cuota para excluir los de matrículas descartadas
        if not df_matriculas_pi_final.empty and not df_primera_cuota_pi_final.empty:
            codigos_matriculas_validas = set(df_matriculas_pi_final["codigo_matricula"].dropna().astype(str).unique())
            total_pagos_antes = len(df_primera_cuota_pi_final)
            df_primera_cuota_pi_final = df_primera_cuota_pi_final[
                df_primera_cuota_pi_final["codigo_matricula"].astype(str).isin(codigos_matriculas_validas)
            ].reset_index(drop=True)
            excluidos = total_pagos_antes - len(df_primera_cuota_pi_final)
            if excluidos > 0:
                logger.info(f"Excluidos {excluidos} pagos de primera cuota por matrículas descartadas (no comienzan con 'P')")
    else:
        logger.warning("No se extrajeron matrículas")
    
    # Cargar matrículas
    if not df_matriculas_pi_final.empty:
        logger.info(f"Cargando {len(df_matriculas_pi_final)} matrículas del {target_date}...")
        try:
            load("matriculas", df_matriculas_pi_final, pk_column="codigo_matricula", upsert=False)
            logger.info(f"✓ Matrículas cargadas exitosamente: {len(df_matriculas_pi_final)} registros")
        except Exception as e:
            logger.error(f"✗ Error al cargar matrículas: {e}")
            raise
    else:
        logger.info(f"No hay matrículas del {target_date} para cargar")

    # ==================== PAGOS REGULARES (FILTRADOS POR FECHA) ====================
    logger.info(f"Procesando hoja Regular Pagos (filtro: {target_date})")
    df_regular_pagos_raw = extract_sheet_pi_3(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_PAGOS"),
        None,
        None,
    )
    if not df_regular_pagos_raw.empty:
        # Filtrar en RAW por Marca temporal antes de transformar
        if "Marca temporal" in df_regular_pagos_raw.columns:
            df_regular_pagos_raw["Marca temporal"] = pd.to_datetime(df_regular_pagos_raw["Marca temporal"], dayfirst=True, errors="coerce")
            total_before = len(df_regular_pagos_raw)
            df_regular_pagos_raw["_fecha_marca"] = df_regular_pagos_raw["Marca temporal"].dt.strftime("%Y-%m-%d")
            df_regular_pagos_raw = df_regular_pagos_raw.loc[
                df_regular_pagos_raw["_fecha_marca"] == target_date
            ].reset_index(drop=True)
            logger.info(f"Pagos regulares RAW filtrados por Marca temporal: {len(df_regular_pagos_raw)} de {total_before} (fecha={target_date})")
        else:
            logger.warning("Columna 'Marca temporal' no encontrada en pagos regulares, no se aplicó filtro de fecha")
        
        df_regular_pagos_final = transform_regular_pagos(df_regular_pagos_raw)
    else:
        logger.warning("No se extrajeron pagos regulares")

    # ==================== CONSOLIDACIÓN Y CARGA DE PAGOS ====================
    logger.info(f"Consolidando pagos (primera cuota + regulares) para {target_date}")
    
    # Obtener conjunto de códigos de matrícula válidos para filtrar pagos
    codigos_matriculas_validas = set()
    if not df_matriculas_pi_final.empty:
        codigos_matriculas_validas = set(df_matriculas_pi_final["codigo_matricula"].dropna().astype(str).unique())
        logger.info(f"Códigos de matrículas válidas para referencia FK: {len(codigos_matriculas_validas)}")
    
    # Filtrar pagos regulares para excluir los de matrículas no válidas
    if not df_regular_pagos_final.empty and codigos_matriculas_validas:
        total_pagos_regulares_antes = len(df_regular_pagos_final)
        df_regular_pagos_final = df_regular_pagos_final[
            df_regular_pagos_final["codigo_matricula"].astype(str).isin(codigos_matriculas_validas)
        ].reset_index(drop=True)
        excluidos_regulares = total_pagos_regulares_antes - len(df_regular_pagos_final)
        if excluidos_regulares > 0:
            logger.warning(f"Excluidos {excluidos_regulares} pagos regulares sin matrícula válida correspondiente")
    
    # Asegurar índices y columnas únicos antes de concatenar
    df_primera_cuota_pi_final = df_primera_cuota_pi_final.reset_index(drop=True)
    df_regular_pagos_final = df_regular_pagos_final.reset_index(drop=True)
    
    # Eliminar columnas duplicadas si existen
    if df_primera_cuota_pi_final.columns.duplicated().any():
        logger.warning("Columnas duplicadas detectadas en df_primera_cuota_pi_final, eliminando...")
        df_primera_cuota_pi_final = df_primera_cuota_pi_final.loc[:, ~df_primera_cuota_pi_final.columns.duplicated()]
    if df_regular_pagos_final.columns.duplicated().any():
        logger.warning("Columnas duplicadas detectadas en df_regular_pagos_final, eliminando...")
        df_regular_pagos_final = df_regular_pagos_final.loc[:, ~df_regular_pagos_final.columns.duplicated()]
    
    # Concatenar solo los DataFrames no vacíos
    dataframes_to_concat = []
    if not df_primera_cuota_pi_final.empty:
        dataframes_to_concat.append(df_primera_cuota_pi_final)
    if not df_regular_pagos_final.empty:
        dataframes_to_concat.append(df_regular_pagos_final)
    
    if dataframes_to_concat:
        df_final_pagos = pd.concat(dataframes_to_concat, ignore_index=True)
    else:
        # Si ambos están vacíos, crear un DataFrame vacío con las columnas esperadas
        df_final_pagos = pd.DataFrame(columns=["codigo_matricula", "monto_pago", "metodo_pago", "moneda", "encargado", "fecha_pago"])
    logger.info(f"Pagos totales consolidados (después de filtrar por FK): {len(df_final_pagos)}")
    
    # Cargar pagos consolidados
    if not df_final_pagos.empty:
        logger.info(f"Cargando {len(df_final_pagos)} pagos del {target_date}...")
        try:
            load("pagos", df_final_pagos, upsert=False)
            logger.info(f"✓ Pagos cargados exitosamente: {len(df_final_pagos)} registros")
        except Exception as e:
            logger.error(f"✗ Error al cargar pagos: {e}")
            raise
    else:
        logger.info(f"No hay pagos del {target_date} para cargar")
    
    # ==================== RESUMEN FINAL ====================
    logger.info("="*60)
    logger.info(f"RESUMEN ETL - Fecha objetivo: {target_date}")
    logger.info(f"  Cursos cargados (UPSERT):      {len(df_cursos_pi_final)}")
    logger.info(f"  Estudiantes cargados (UPSERT): {len(df_es_final)}")
    logger.info(f"  Matrículas del {target_date}:     {len(df_matriculas_pi_final)}")
    logger.info(f"  Pagos del {target_date}:          {len(df_final_pagos)}")
    logger.info("="*60)
    logger.info("✓ ETL COMPLETADO EXITOSAMENTE")


if __name__ == "__main__":
    run_pipeline()
