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
    today = date(2026,1,15)
    yesterday = today - timedelta(days=1)
    target_date = yesterday.strftime("%Y-%m-%d")

    logger.info(f"===== ETL DIARIO | Fecha objetivo: {target_date} =====")

    # inicializar dfs
    df_cursos_pi_final = pd.DataFrame()
    df_matriculas_pi_final = pd.DataFrame()
    df_primera_cuota_pi_final = pd.DataFrame()
    df_regular_pagos_final = pd.DataFrame()
    df_es_final = pd.DataFrame()

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

    # ==================== MATRICULAS (FILTRADAS POR FECHA) ====================
    logger.info(f"Procesando hoja Matriculas (filtro: {target_date})")
    df_matriculas_pi_raw = extract_sheet_pi_2(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_MATRICULAS"),
        None,
        None,
    )
    if not df_matriculas_pi_raw.empty:
        df_matriculas_pi_final = transform_matriculas(df_matriculas_pi_raw)
        df_primera_cuota_pi_final = transform_pagos_primera_cuota(df_matriculas_pi_raw)
        
        # Filtrar solo matrículas del día anterior
        if "fecha_matricula" in df_matriculas_pi_final.columns:
            total_before = len(df_matriculas_pi_final)
            df_matriculas_pi_final = df_matriculas_pi_final.loc[
                df_matriculas_pi_final["fecha_matricula"] == target_date
            ].reset_index(drop=True)
            logger.info(f"Matrículas filtradas: {len(df_matriculas_pi_final)} de {total_before} (fecha={target_date})")
        else:
            logger.warning("Columna 'fecha_matricula' no encontrada, no se aplicó filtro de fecha")
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
        df_regular_pagos_final = transform_regular_pagos(df_regular_pagos_raw)
        
        # Filtrar solo pagos del día anterior
        if "fecha_pago" in df_regular_pagos_final.columns:
            total_before = len(df_regular_pagos_final)
            df_regular_pagos_final = df_regular_pagos_final.loc[
                df_regular_pagos_final["fecha_pago"] == target_date
            ].reset_index(drop=True)
            logger.info(f"Pagos regulares filtrados: {len(df_regular_pagos_final)} de {total_before} (fecha={target_date})")
        else:
            logger.warning("Columna 'fecha_pago' no encontrada en pagos regulares, no se aplicó filtro de fecha")
    else:
        logger.warning("No se extrajeron pagos regulares")

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

    # ==================== CONSOLIDACIÓN Y CARGA DE PAGOS ====================
    logger.info(f"Consolidando pagos (primera cuota + regulares) para {target_date}")
    df_final_pagos = pd.concat(
        [df_primera_cuota_pi_final, df_regular_pagos_final], ignore_index=True
    )
    
    # Filtrar pagos consolidados por fecha
    if "fecha_pago" in df_final_pagos.columns and not df_final_pagos.empty:
        total_before = len(df_final_pagos)
        df_final_pagos = df_final_pagos.loc[
            df_final_pagos["fecha_pago"] == target_date
        ].reset_index(drop=True)
        logger.info(f"Pagos consolidados filtrados: {len(df_final_pagos)} de {total_before} (fecha={target_date})")
    else:
        logger.info(f"Pagos totales consolidados: {len(df_final_pagos)}")
    
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
