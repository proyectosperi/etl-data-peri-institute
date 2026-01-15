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

logger = get_logger("PIPELINE")

load_dotenv()


def run_pipeline(year=None, month=None):
    """ETL diario (solo extracción/transformación y filtrado).

    Esta versión NO ejecuta cargas. Las llamadas a `load(...)` están
    comentadas para que puedas verificar los filtros por fecha.
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

    # HOJA 1 – CURSOS
    logger.info("Procesando hoja Cursos")
    df_cursos_pi_raw = extract_sheet_pi_1(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_CURSOS"),
        None,
        None,
    )
    if not df_cursos_pi_raw.empty:
        # No filtrar por fecha; los cursos deben tratarse vía upsert.
        df_cursos_pi_final = transform_cursos(df_cursos_pi_raw)
    logger.info(f"Cursos para {target_date}: {len(df_cursos_pi_final)}")

    # HOJA 2 – MATRICULAS
    logger.info("Procesando hoja Matriculas")
    df_matriculas_pi_raw = extract_sheet_pi_2(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_MATRICULAS"),
        None,
        None,
    )
    if not df_matriculas_pi_raw.empty:
        df_matriculas_pi_final = transform_matriculas(df_matriculas_pi_raw)
        df_primera_cuota_pi_final = transform_pagos_primera_cuota(df_matriculas_pi_raw)
        if "fecha_matricula" in df_matriculas_pi_final.columns:
            df_matriculas_pi_final = df_matriculas_pi_final.loc[
                df_matriculas_pi_final["fecha_matricula"] == target_date
            ].reset_index(drop=True)
    logger.info(f"Matriculas para {target_date}: {len(df_matriculas_pi_final)}")

    # HOJA 3 – PAGOS REGULARES
    logger.info("Procesando hoja Regular Pagos")
    df_regular_pagos_raw = extract_sheet_pi_3(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_PAGOS"),
        None,
        None,
    )
    if not df_regular_pagos_raw.empty:
        df_regular_pagos_final = transform_regular_pagos(df_regular_pagos_raw)
        if "fecha_pago" in df_regular_pagos_final.columns:
            df_regular_pagos_final = df_regular_pagos_final.loc[
                df_regular_pagos_final["fecha_pago"] == target_date
            ].reset_index(drop=True)
    logger.info(f"Pagos regulares para {target_date}: {len(df_regular_pagos_final)}")

    # HOJA 4 – ESTUDIANTES
    logger.info("Procesando hoja Estudiantes")
    df_alumnos_pi_raw = extract_sheet_pi_1(
        os.getenv("Matricula_PI_ID"),
        os.getenv("WORKSHEET_ESTUDIANTES"),
        None,
        None,
    )
    if not df_alumnos_pi_raw.empty:
        df_es_final = transform_estudiantes(df_alumnos_pi_raw)
    logger.info(f"Estudiantes transformados: {len(df_es_final)}")

    # CONSOLIDACIÓN PAGOS
    df_final_pagos = pd.concat(
        [df_primera_cuota_pi_final, df_regular_pagos_final], ignore_index=True
    )
    if "fecha_pago" in df_final_pagos.columns:
        df_final_pagos = df_final_pagos.loc[
            df_final_pagos["fecha_pago"] == target_date
        ].reset_index(drop=True)
    logger.info(f"Pagos totales para {target_date}: {len(df_final_pagos)}")

    # NOTA: Las cargas a la base de datos están deshabilitadas en esta versión.
    # Si quieres habilitarlas, descomenta las llamadas a `load(...)` abajo.
    # -----------------------------
    # Ejemplo (comentado) - upsert para cursos (como estudiantes):
    # if not df_cursos_pi_final.empty:
    #     load("cursos", df_cursos_pi_final, upsert=True, pk_column="codigo_curso")
    # -----------------------------


if __name__ == "__main__":
    run_pipeline()
