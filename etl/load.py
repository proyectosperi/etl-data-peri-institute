from supabase import create_client
from dotenv import load_dotenv
import os
import pandas as pd
import math
import numpy as np
from datetime import datetime
from decimal import Decimal
from logger import get_logger
from postgrest.exceptions import APIError

logger = get_logger("LOAD")

load_dotenv()

# Prefer service role key for server-side writes (must be kept secret).
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Faltan variables de entorno SUPABASE_URL o SUPABASE_KEY/SUPABASE_SERVICE_ROLE_KEY")

if SUPABASE_SERVICE_ROLE_KEY:
    logger.info("Usando SUPABASE_SERVICE_ROLE_KEY para cargas (server-side).")
else:
    logger.warning("No se encontró SUPABASE_SERVICE_ROLE_KEY; usando SUPABASE_KEY. Asegúrate de que la política de RLS permite inserciones.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Columnas requeridas por tabla (si están ausentes/null, se eliminará el registro antes de insertar)
REQUIRED_COLUMNS_BY_TABLE = {
    "pagos": ["fecha_pago"],
    # Añadir otras tablas si es necesario, p.ej.:
    # "matriculas": ["codigo_matricula"],
}

def load(table_name: str, df: pd.DataFrame, abort_on_error: bool = True, pk_column: str = None, dedupe_df: bool = True, drop_missing_students: bool = False, drop_missing_matriculas: bool = False, required_columns: list = None, upsert: bool = False):
    
    logger.info(f"Cargando {len(df)} registros en Supabase tabla: {table_name}")
    logger.info(f"Columnas recibidas para carga: {df.columns.tolist()}")

    # Verificaciones previas: duplicados en DataFrame y (opcional) en la DB
    if pk_column:
        if pk_column not in df.columns:
            logger.warning(f"pk_column especificado ('{pk_column}') no está en el DataFrame. Se omitirá la verificación de duplicados.")
        else:
            # Duplicados dentro del DataFrame
            dup_mask = df.duplicated(subset=[pk_column], keep=False)
            if dup_mask.any():
                dup_vals = df.loc[dup_mask, pk_column].unique().tolist()
                if dedupe_df:
                    logger.warning(f"Duplicados detectados en DataFrame para PK '{pk_column}': {dup_vals}. Se eliminarán duplicados quedando con la última fila por PK (dedupe_df=True).")
                    df = df.drop_duplicates(subset=[pk_column], keep="last").reset_index(drop=True)
                else:
                    logger.error(f"Duplicados detectados en DataFrame para PK '{pk_column}': {dup_vals}")
                    raise ValueError(f"Duplicados en DataFrame para PK '{pk_column}': {dup_vals}")
            # Obtener lista de claves a operar
            keys = [k for k in df[pk_column].dropna().unique().tolist()]

            # Intentar verificar claves existentes en la tabla (no destructivo), en lotes
            try:
                if keys:
                    CHUNK_SIZE = 500
                    existing_vals = []
                    for i in range(0, len(keys), CHUNK_SIZE):
                        chunk = keys[i:i+CHUNK_SIZE]
                        resp = supabase.table(table_name).select(pk_column).in_(pk_column, chunk).execute()
                        try:
                            part = resp.data if hasattr(resp, "data") else (resp.get("data") if isinstance(resp, dict) else None)
                        except Exception:
                            part = None

                        if part:
                            existing_vals.extend([r.get(pk_column) if isinstance(r, dict) else r[pk_column] for r in part])

                    if existing_vals:
                        logger.error(f"Claves ya existentes en la tabla '{table_name}' para columna '{pk_column}': {existing_vals}")
                        raise ValueError(f"Claves ya existentes en la tabla '{table_name}': {existing_vals}")
            except Exception as e:
                logger.warning(f"No se pudo verificar claves existentes en DB (continuando): {e}")

            # Validación de claves foráneas comunes: matriculas.codigo_estudiante -> estudiantes.codigo_estudiante
            try:
                if table_name == "matriculas" and "codigo_estudiante" in df.columns:
                    student_keys = [k for k in df["codigo_estudiante"].dropna().unique().tolist()]
                    missing_students = []
                    if student_keys:
                        CHUNK_SIZE = 500
                        existing_students = []
                        for i in range(0, len(student_keys), CHUNK_SIZE):
                            chunk = student_keys[i:i+CHUNK_SIZE]
                            resp = supabase.table("estudiantes").select("codigo_estudiante").in_("codigo_estudiante", chunk).execute()
                            try:
                                part = resp.data if hasattr(resp, "data") else (resp.get("data") if isinstance(resp, dict) else None)
                            except Exception:
                                part = None

                            if part:
                                existing_students.extend([r.get("codigo_estudiante") if isinstance(r, dict) else r["codigo_estudiante"] for r in part])

                        missing_students = list(set(student_keys) - set(existing_students))

                    if missing_students:
                        logger.error(f"Faltan estudiantes referenciados en 'matriculas' no presentes en 'estudiantes': {missing_students}")
                        if drop_missing_students:
                            try:
                                # Guardar los registros que serán eliminados en un CSV para auditoría
                                out_dir = os.path.join(os.getcwd(), "etl", "output")
                                os.makedirs(out_dir, exist_ok=True)
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                out_path = os.path.join(out_dir, f"matriculas_missing_students_{timestamp}.csv")
                                mask_missing = df["codigo_estudiante"].isin(missing_students)
                                df_missing = df.loc[mask_missing]
                                df_missing.to_csv(out_path, index=False)
                                logger.info(f"Registros de 'matriculas' con estudiantes faltantes guardados en: {out_path}. Serán eliminados antes de la carga.")
                                # Eliminar filas faltantes del DataFrame para continuar
                                df = df.loc[~mask_missing].reset_index(drop=True)
                                # Recompute data and keys after removal
                                data = df.to_dict(orient="records")
                                keys = [k for k in df[pk_column].dropna().unique().tolist()] if pk_column and pk_column in df.columns else []
                            except Exception as e:
                                logger.error(f"Error al guardar/eliminar registros faltantes: {e}")
                                raise
                        else:
                            raise ValueError(f"Faltan estudiantes referenciados: {missing_students}")
            except Exception as e:
                logger.warning(f"Error verificando claves foraneas de 'matriculas' (continuando): {e}")

    # Si se definen columnas requeridas, eliminar filas con nulos en esas columnas
    cols_required = required_columns if required_columns is not None else REQUIRED_COLUMNS_BY_TABLE.get(table_name, [])
    if cols_required:
        missing_mask = pd.DataFrame(df)[cols_required].isna().any(axis=1)
        if missing_mask.any():
            removed_df = df.loc[missing_mask]
            # Guardar backup de registros eliminados
            out_dir = os.path.join(os.getcwd(), "etl", "output")
            os.makedirs(out_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join(out_dir, f"{table_name}_removed_nulls_{timestamp}.csv")
            try:
                removed_df.to_csv(out_path, index=False)
                logger.info(f"Se eliminaron {len(removed_df)} registros de '{table_name}' por nulos en columnas requeridas {cols_required}. Backup: {out_path}")
            except Exception as e:
                logger.warning(f"No se pudo guardar backup de registros eliminados por nulos: {e}")

            # Filtrar df para continuar con los registros válidos
            df = df.loc[~missing_mask].reset_index(drop=True)

    # Validación de claves foráneas para pagos (ejecutar siempre, incluso si no se pasó pk_column)
    try:
        if table_name == "pagos" and "codigo_matricula" in df.columns:
            payment_keys = [k for k in df["codigo_matricula"].dropna().unique().tolist()]
            logger.info(f"Validando FK 'matriculas' para pagos: {len(payment_keys)} claves a verificar")
            missing_payments = []
            if payment_keys:
                CHUNK_SIZE = 500
                existing_mats = []
                for i in range(0, len(payment_keys), CHUNK_SIZE):
                    chunk = payment_keys[i:i+CHUNK_SIZE]
                    resp = supabase.table("matriculas").select("codigo_matricula").in_("codigo_matricula", chunk).execute()
                    try:
                        part = resp.data if hasattr(resp, "data") else (resp.get("data") if isinstance(resp, dict) else None)
                    except Exception:
                        part = None

                    if part:
                        existing_mats.extend([r.get("codigo_matricula") if isinstance(r, dict) else r["codigo_matricula"] for r in part])

                logger.info(f"Encontradas en DB (muestras hasta 5): {existing_mats[:5]} (total {len(existing_mats)})")
                missing_payments = list(set(payment_keys) - set(existing_mats))

            if missing_payments:
                logger.error(f"Faltan matriculas referenciadas en 'pagos' no presentes en 'matriculas' (total {len(missing_payments)}). Ejemplos: {missing_payments[:5]}")
                if drop_missing_matriculas:
                    try:
                        out_dir = os.path.join(os.getcwd(), "etl", "output")
                        os.makedirs(out_dir, exist_ok=True)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        out_path = os.path.join(out_dir, f"pagos_missing_matriculas_{timestamp}.csv")
                        mask_missing = df["codigo_matricula"].isin(missing_payments)
                        df_missing = df.loc[mask_missing]
                        df_missing.to_csv(out_path, index=False)
                        logger.info(f"Registros de 'pagos' con matriculas faltantes guardados en: {out_path}. Serán eliminados antes de la carga. Cantidad: {len(df_missing)}")
                        df = df.loc[~mask_missing].reset_index(drop=True)
                    except Exception as e:
                        logger.error(f"Error al guardar/eliminar registros faltantes en 'pagos': {e}")
                        raise
                else:
                    raise ValueError(f"Faltan matriculas referenciadas: {missing_payments}")
    except Exception as e:
        logger.warning(f"Error verificando claves foraneas de 'pagos' (continuando): {e}")

    data = df.to_dict(orient="records")

    def _sanitize_value(v):
        # None / NaN
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass

        # unwrap numpy / pandas scalars
        if hasattr(v, "item"):
            try:
                v = v.item()
            except Exception:
                pass

        # numpy arrays -> lists
        if isinstance(v, np.ndarray):
            return v.tolist()

        # floats: filter inf/-inf
        if isinstance(v, (float, np.floating)):
            try:
                fv = float(v)
                if not math.isfinite(fv):
                    return None
                return fv
            except Exception:
                return None

        # ints
        if isinstance(v, (int, np.integer)):
            return int(v)

        # bools
        if isinstance(v, (bool, np.bool_)):
            return bool(v)

        # datetimes / timestamps
        if isinstance(v, (datetime, pd.Timestamp)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)

        # Decimal
        if isinstance(v, Decimal):
            try:
                return float(v)
            except Exception:
                return None

        return v


    # Sanear datos antes de la inserción masiva
    cleaned_data = [{k: _sanitize_value(v) for k, v in rec.items()} for rec in data]

    try:
        if upsert:
            logger.info(f"Ejecutando upsert en tabla '{table_name}' (upsert=True).")
            supabase.table(table_name).upsert(cleaned_data).execute()
        else:
            supabase.table(table_name).insert(cleaned_data).execute()
        logger.info("Carga masiva completada.")
        return
    except Exception:
        logger.exception("Error en carga masiva a Supabase.")

        if abort_on_error:
            logger.error("Abortando carga completa por fallo en inserción masiva (abort_on_error=True). No se realizarán inserciones parciales.")
            raise

        logger.warning("Inserción masiva falló; intentando inserción registro a registro para aislar el conflicto (abort_on_error=False).")

    # Si llegamos acá, se solicitó intentar por registro (modo debug).
    for idx, rec in enumerate(data):
        rec_clean = {k: _sanitize_value(v) for k, v in rec.items()}

        try:
            supabase.table(table_name).insert(rec_clean).execute()
        except Exception as e2:
            logger.error(f"Registro conflictivo índice {idx}: {rec_clean}")
            logger.error(f"Error al insertar registro índice {idx}: {e2}")
            # Detener en el primer error: no permitimos continuar con inserciones parciales
            raise


__all__ = ["load"]
