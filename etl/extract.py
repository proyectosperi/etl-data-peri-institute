import re
import unicodedata
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from logger import get_logger


logger = get_logger("EXTRACT")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def get_gspread_client():
    credentials = Credentials.from_service_account_info(
        json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")),
        scopes=SCOPES
    )
    return gspread.authorize(credentials)

def get_records_with_header(ws, header_row=2):
    """
    Lee una hoja de Google Sheets usando una fila específica como cabecera
    """
    values = ws.get_all_values()

    if len(values) < header_row:
        raise ValueError("La hoja no tiene suficientes filas para usar como cabecera")

    headers = values[header_row - 1]
    data = values[header_row:]

    return pd.DataFrame(data, columns=headers)


def get_all_records_robust(ws):
    """Leer toda la hoja y construir registros robustos.
    - Detecta la primera fila no vacía como encabezado.
    - Rellena encabezados vacíos con `col_{i}` y asegura nombres únicos.
    - Convierte cadenas numéricas a int/float para preservar seriales de fecha.
    - Omite filas completamente vacías.
    """
    values = ws.get_all_values()
    if not values:
        return []

    # encontrar la primera fila que parezca encabezado (alguna celda no vacía)
    header_idx = next((i for i, r in enumerate(values) if any(str(c).strip() for c in r)), 0)
    raw_headers = [str(h).strip() for h in values[header_idx]]

    # normalizar y hacer únicos los nombres de columnas
    seen = {}
    headers = []
    for j, h in enumerate(raw_headers):
        name = h or f"col_{j}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        headers.append(name)

    data_rows = values[header_idx + 1 :]
    records = []
    num_cols = len(headers)

    for row in data_rows:
        # asegurar longitud
        row_extended = list(row) + [""] * max(0, num_cols - len(row))
        rec = {}
        for i, cell in enumerate(row_extended[:num_cols]):
            if cell is None:
                rec[headers[i]] = None
                continue
            s = str(cell).strip()
            if s == "":
                rec[headers[i]] = None
                continue

            # intentar convertir a número (int o float)
            if re.fullmatch(r"-?\d+", s):
                try:
                    rec[headers[i]] = int(s)
                    continue
                except Exception:
                    pass
            if re.fullmatch(r"-?\d+\.\d+", s):
                try:
                    rec[headers[i]] = float(s)
                    continue
                except Exception:
                    pass

            # conservar string
            rec[headers[i]] = s

        # ignorar filas vacías
        if all(v is None for v in rec.values()):
            continue

        records.append(rec)

    return records


def _normalize_col_name(name):
    s = str(name or "")
    s = unicodedata.normalize('NFKD', s)
    s = s.encode('ascii', 'ignore').decode('ascii')
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _find_column(df, candidates):
    """Buscar columna en df comparando nombres normalizados contra candidatos.
    Retorna el nombre original de la columna si se encuentra, o None.
    """
    norm_map = { _normalize_col_name(c): c for c in df.columns }
    for cand in candidates:
        n = _normalize_col_name(cand)
        if n in norm_map:
            return norm_map[n]
    # intentar coincidencia por substring (caso 'fecha' en 'fecha_de_pago')
    for cand in candidates:
        n = _normalize_col_name(cand)
        for k, orig in norm_map.items():
            if n in k or k in n:
                return orig
    return None
    


def normalize_columns(df):
    """Renombrar columnas del dataframe a nombres canónicos cuando sea posible.
    Evita KeyError al referirse a nombres esperados en el código.
    """
    mapping = {
        "Fecha de pago": ["Fecha de pago", "fecha de pago", "fecha_pago", "fechadepago", "fechapago"],
        "FECHA_P": ["FECHA_P", "FECHA P", "fecha_p", "fecha p", "fecha_p"],
        "FechaEntrega": ["FechaEntrega", "fecha entrega", "fecha_entrega", "fechaentrega"],
        "Estado": ["Estado", "estado", "ESTADO"],
    }

    for canonical, candidates in mapping.items():
        found = _find_column(df, candidates)
        if found and found != canonical:
            try:
                df.rename(columns={found: canonical}, inplace=True)
            except Exception:
                logger.debug(f"No se pudo renombrar {found} -> {canonical}")

    return df

def extract_sheet_pi_1(sheet_id, worksheet_name, year, month):
    logger.info(f"Extrayendo datos | Sheet: {sheet_id}")
    """
    target_year = year
    target_month = month
    """
    # =========================
    # CONEXIÓN GOOGLE SHEETS
    # =========================
    ws = (
        get_gspread_client()
        .open_by_key(sheet_id)
        .worksheet(worksheet_name)
    )

    # Asegurar que la segunda fila (fila index 1) se use siempre como cabecera
    values = ws.get_all_values()
    if len(values) >= 2:
        headers = [str(h).strip() for h in values[1]]
        data = values[2:]
        df = pd.DataFrame(data, columns=headers)
    else:
        # fallback: usar la función auxiliar si no hay suficientes filas
        df = get_records_with_header(ws, header_row=2)

    logger.info(f"Registros totales extraídos: {len(df)}")

    
    # =========================
    # NORMALIZACIÓN BÁSICA
    # =========================
    df.columns = df.columns.str.strip()
    df = normalize_columns(df)
    logger.info(f"Cabeceras detectadas: {list(df.columns)}")


    # (Filtro de periodo anterior eliminado aquí; aplicar en transform si hace falta)
    # =========================
    # SAMPLE PARA VERIFICACIÓN
    # =========================
    if not df.empty:
        logger.info("Sample de registros extraídos:")
        logger.info(
            "\n" + df.head(5).to_string(index=False)
        )
    else:
        logger.warning("No hay registros luego de aplicar los filtros")

    return df

def extract_sheet_pi_2(sheet_id, worksheet_name, year, month):
    logger.info(f"Extrayendo datos | Sheet: {sheet_id}")
    """
    target_year = year
    target_month = month
    """
    # =========================
    # CONEXIÓN GOOGLE SHEETS
    # =========================
    ws = (
        get_gspread_client()
        .open_by_key(sheet_id)
        .worksheet(worksheet_name)
    )

    # Usar la fila 3 (index 2) como cabecera cuando sea posible
    values = ws.get_all_values()
    if len(values) >= 3:
        headers = [str(h).strip() for h in values[2]]
        data = values[3:]
        df = pd.DataFrame(data, columns=headers)
    else:
        records = get_all_records_robust(ws)
        df = pd.DataFrame(records)

    logger.info(f"Registros totales extraídos: {len(df)}")

    # =========================
    # NORMALIZACIÓN BÁSICA
    # =========================
    df.columns = df.columns.str.strip()
    df = normalize_columns(df)
    logger.info(f"Cabeceras detectadas: {list(df.columns)}")


    # (Filtro de periodo anterior eliminado aquí; aplicar en transform si hace falta)
    # =========================
    # SAMPLE PARA VERIFICACIÓN
    # =========================
    if not df.empty:
        logger.info("Sample de registros extraídos:")
        logger.info(
            "\n" + df.head(5).to_string(index=False)
        )
    else:
        logger.warning("No hay registros luego de aplicar los filtros")

    return df

def extract_sheet_pi_3(sheet_id, worksheet_name, year, month):
    logger.info(f"Extrayendo datos | Sheet: {sheet_id}")
    """
    target_year = year
    target_month = month
    """
    # =========================
    # CONEXIÓN GOOGLE SHEETS
    # =========================
    ws = (
        get_gspread_client()
        .open_by_key(sheet_id)
        .worksheet(worksheet_name)
    )

    # Usar la fila 6 (index 5) como cabecera cuando sea posible
    values = ws.get_all_values()
    if len(values) >= 6:
        headers = [str(h).strip() for h in values[5]]
        data = values[6:]
        df = pd.DataFrame(data, columns=headers)
    else:
        records = get_all_records_robust(ws)
        df = pd.DataFrame(records)

    logger.info(f"Registros totales extraídos: {len(df)}")

    # =========================
    # NORMALIZACIÓN BÁSICA
    # =========================
    df.columns = df.columns.str.strip()
    df = normalize_columns(df)
    logger.info(f"Cabeceras detectadas: {list(df.columns)}")

    # localizar columna de fecha para esta hoja (varias variantes posibles)
    fecha_col_pi2 = _find_column(df, ["Fecha de pago", "fecha_pago", "FechaPago", "Fecha", "fecha"])
    if fecha_col_pi2 is None:
        logger.warning("No se encontró columna 'Fecha de pago' en la hoja; se creará columna vacía 'Fecha de pago'.")
        df["Fecha de pago"] = None
        fecha_col_pi2 = "Fecha de pago"

    # (Parseo de fechas y filtros específicos eliminados; hacer en fase de transformaciones)
    # =========================
    # SAMPLE PARA VERIFICACIÓN
    # =========================
    if not df.empty:
        logger.info("Sample de registros extraídos:")
        logger.info(
            "\n" + df.head(5).to_string(index=False)
        )
    else:
        logger.warning("No hay registros luego de aplicar los filtros")

    return df


__all__ = [
    "extract_sheet_pi_1",
    "extract_sheet_pi_2",
    "extract_sheet_pi_3",
]
