import pandas as pd
import re
from logger import get_logger

logger = get_logger("TRANSFORM")

def transform_cursos(df):
    logger.info("Transformando hoja de cursos Peri Institute")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df
    # Asegurar que columnas de fecha estén en formato datetime
    date_cols = ["FECHA DE INICIO", "FECHA DE TERMINO", "fecha"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
            n_null = int(df[c].isna().sum())
            logger.info(f"Columna '{c}' convertida a datetime; nulos: {n_null}")
    def extraer_codigo(value):
        return value.split(" ", 1)[0]

    # =================
    df_transformed = pd.DataFrame({
        "codigo_curso": df["CÓDIGO_C"].astype(str),
        "nombre_curso": df["NOMBRE_C"].astype(str),
        "numero_modulo": df["I1"].astype(int),
        "fecha_inicio": df["FECHA DE INICIO"].dt.strftime("%Y-%m-%d"),
        "codigo_profesor": df["PROFESOR"].apply(extraer_codigo).astype(str),
        "horarios": df["HORARIOS"].astype(str),
    })
    # Eliminar duplicados por PK en la capa de transformación si aparecen
    if "codigo_curso" in df_transformed.columns:
        dup_mask = df_transformed.duplicated(subset=["codigo_curso"], keep=False)
        if dup_mask.any():
            dup_vals = df_transformed.loc[dup_mask, "codigo_curso"].unique().tolist()
            logger.warning(f"Duplicados detectados en transform_cursos para 'codigo_curso': {dup_vals}. Se eliminarán duplicados manteniendo la última fila.")
            df_transformed = df_transformed.drop_duplicates(subset=["codigo_curso"], keep="last").reset_index(drop=True)
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed




def transform_matriculas(df):
    logger.info("Transformando hoja de ventas Peri Institute - Matriculas")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df
    # Asegurar columnas de fecha como datetime
    for c in ["Marca temporal", "Fecha de pago de la primera cuota", "Fecha de pago"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
            n_null = int(df[c].isna().sum())
            logger.info(f"Columna '{c}' convertida a datetime; nulos: {n_null}")
    def extraer_codigo_proyecto(valor):
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return valor

        texto = str(valor).strip()

        # Solo transformar si empieza con P
        if texto.startswith("P"):
            return texto.split(" ", 1)[0]

        return texto


    def normalizar_num_cursos(curso_matricula, num_cursos):
        # Manejo de nulos
        if curso_matricula is None or (isinstance(curso_matricula, float) and pd.isna(curso_matricula)):
            return 0

        texto = str(curso_matricula).strip()

        # Si empieza con P, se respeta el valor original
        if texto.startswith("P"):
            return num_cursos

        return 0


    

    # Usar explícitamente 'Fecha de pago de la primera cuota' como fecha de matrícula.
    # Si no existe la columna, dejar como NA.
    if "Fecha de pago de la primera cuota" in df.columns:
        fecha_matricula_col = df["Fecha de pago de la primera cuota"].dt.strftime("%Y-%m-%d")
    else:
        fecha_matricula_col = pd.Series([pd.NA] * len(df))

    df_transformed = pd.DataFrame({
        "codigo_matricula": df["Código de matrícula"].astype(str),
        "codigo_curso": df["Cursos de matrícula"].apply(extraer_codigo_proyecto).astype(str),
        "num_cursos": df.apply(lambda row: normalizar_num_cursos(row["Cursos de matrícula"], row["num cursos"]), axis=1).astype(int),
        "fecha_matricula": fecha_matricula_col,
        "condicion_alumno": df["Condición del alumno"].astype(str),
        "codigo_estudiante": df["Código de estudiante FINAL"].astype(str),
        "valor_matricula": pd.to_numeric(df["Monto de Pago"], errors="coerce").fillna(0).round(2)
    })
    # Eliminar duplicados por PK en la capa de transformación si aparecen
    if "codigo_matricula" in df_transformed.columns:
        dup_mask = df_transformed.duplicated(subset=["codigo_matricula"], keep=False)
        if dup_mask.any():
            dup_vals = df_transformed.loc[dup_mask, "codigo_matricula"].unique().tolist()
            logger.warning(f"Duplicados detectados en transform_matriculas para 'codigo_matricula': {dup_vals}. Se eliminarán duplicados manteniendo la última fila.")
            df_transformed = df_transformed.drop_duplicates(subset=["codigo_matricula"], keep="last").reset_index(drop=True)

    # Filtrar filas cuyo codigo_curso no comienza con 'P' (no son proyectos) — no las incluimos en la carga
    if "codigo_curso" in df_transformed.columns:
        mask_p = df_transformed["codigo_curso"].str.startswith("P", na=False)
        removed = len(df_transformed) - int(mask_p.sum())
        if removed > 0:
            logger.info(f"Se excluyen {removed} registros de 'matriculas' cuyo 'codigo_curso' no comienza con 'P'.")
            df_transformed = df_transformed.loc[mask_p].reset_index(drop=True)
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed

def transform_pagos_primera_cuota(df):
    logger.info("Transformando hoja de pagos primera cuota Peri Institute")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df
    # Convertir fecha de primera cuota a datetime
    if "Fecha de pago de la primera cuota" in df.columns:
        df["Fecha de pago de la primera cuota"] = pd.to_datetime(df["Fecha de pago de la primera cuota"], dayfirst=True, errors="coerce")
        n_null = int(df["Fecha de pago de la primera cuota"].isna().sum())
        logger.info(f"Columna 'Fecha de pago de la primera cuota' convertida a datetime; nulos: {n_null}")
     # =========================
    # NORMALIZADOR DE CUENTAS
    # =========================
    ACCOUNT_MAP = {
    "BANCO DE LA NACIÓN": "Banco de la Nación",
    "SCOTIABANK": "Scotiabank",
    "INTERBANK": "Interbank",
    "YAPE": "Yape",
    "PLIN": "Plin",
    "BBVA": "BBVA",
    "BCP": "BCP",
    "TARJETA LINK": "Tarjeta LINK",
    "PAYPAL": "Paypal",
    "BANCO DE MÉXICO": "Banco de México",
    "BANCO DE MEXICO": "Banco de México",
    "BANCO DE ECUADOR": "Banco de Ecuador",
    "BANCO DE COLOMBIA": "Banco de Colombia",
    "BANCO DE CHILE": "Banco de Chile",
    "OTROS": "Sin Especificar"
    }
    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())

    df_transformed = pd.DataFrame({
        "codigo_matricula": df["Código de matrícula"].astype(str),
        "monto_pago": pd.to_numeric(df["Primera Cuota"], errors="coerce").fillna(0).round(2),
        "metodo_pago": df["Método de Pago"].apply(normalize_account),
        "moneda": df["Moneda"].astype(str),
        "encargado": df["Encargado de Registro"].astype(str),
        "fecha_pago": df["Fecha de pago de la primera cuota"].dt.strftime("%Y-%m-%d"),
        
    })
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed



def transform_regular_pagos(df):
    logger.info("Transformando hoja de ventas Peri Institute")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df

    # Asegurar columnas de fecha como datetime
    if "Fecha de pago" in df.columns:
        df["Fecha de pago"] = pd.to_datetime(df["Fecha de pago"], dayfirst=True, errors="coerce")
        n_null = int(df["Fecha de pago"].isna().sum())
        logger.info(f"Columna 'Fecha de pago' convertida a datetime; nulos: {n_null}")

    # =========================
    # NORMALIZADOR DE CUENTAS
    # =========================
    ACCOUNT_MAP = {
    "BANCO DE LA NACIÓN": "Banco de la Nación",
    "SCOTIABANK": "Scotiabank",
    "INTERBANK": "Interbank",
    "YAPE": "Yape",
    "PLIN": "Plin",
    "BBVA": "BBVA",
    "BCP": "BCP",
    "TARJETA LINK": "Tarjeta LINK",
    "PAYPAL": "Paypal",
    "BANCO DE MÉXICO": "Banco de México",
    "BANCO DE ECUADOR / P": "Banco de Ecuador",
    "BANCO DE MEXICO": "Banco de México",
    "BANCO DE MÉXICO / P": "Banco de México",
    "BANCO DE ECUADOR": "Banco de Ecuador",
    "BANCO DE COLOMBIA": "Banco de Colombia",
    "BANCO DE CHILE": "Banco de Chile",
    "OTROS": "Sin Especificar"
    }


    def normalize_account(value):
        if not value:
            return None

        key = str(value).strip().upper()
        return ACCOUNT_MAP.get(key, value.title())
    def currency_fixed(value):
        if value == "Banco de México" or value == "Banco de Mexico" or value == "Banco de México / P" or value == "Banco de Ecuador / P":
            return "MXN"
        elif value == "Banco de Ecuador" or value == "PAYPAL" or value == "Paypal":
            return "USD"
        elif value == "Banco de Chile":
            return "CLP"
        else:
            return "PEN"

    

    df_transformed = pd.DataFrame({
        "codigo_matricula": df["Código de matrícula"].astype(str),
        "monto_pago": pd.to_numeric(df["Monto de Pago"], errors="coerce").fillna(0).round(2),
        "metodo_pago": df["Método de Pago"].apply(normalize_account),
        "moneda": df["Método de Pago"].apply(currency_fixed).astype(str),
        "encargado": df["Encargado de Registro"].astype(str),
        "fecha_pago": df["Fecha de pago"].dt.strftime("%Y-%m-%d"),
    })
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed


def transform_estudiantes(df):
    logger.info("Transformando hoja de estudiantes Peri Institute")

    if df.empty:
        logger.warning("DataFrame vacío, no hay datos para transformar")
        return df
    

    def detectar_pais_telefono(value):
        if value is None:
            return {
                "pais": "Desconocido",
                "codigo_pais": None,
                "numero_e164": None
            }

        # 1️⃣ Normalizar: solo dígitos
        num = re.sub(r"\D", "", str(value))

        # 2️⃣ Tabla de prefijos (orden importa: más largos primero)
        paises = [
            # América Latina
            ("Argentina", "54", lambda n: n.startswith("549")),
            ("Chile", "56", lambda n: n.startswith("569")),
            ("Perú", "51", lambda n: n.startswith("51")),
            ("Colombia", "57", lambda n: n.startswith("57")),
            ("Ecuador", "593", lambda n: n.startswith("593")),
            ("Bolivia", "591", lambda n: n.startswith("591")),
            ("Panamá", "507", lambda n: n.startswith("507")),
            ("México", "52", lambda n: n.startswith("521") or n.startswith("52")),
            ("Brasil", "55", lambda n: n.startswith("55")),

            # Estados Unidos / territorios
            ("Estados Unidos / Puerto Rico", "1", lambda n: len(n) == 11 and n.startswith("1")),

            # Europa
            ("Italia", "39", lambda n: n.startswith("39")),
            ("España", "34", lambda n: n.startswith("34")),
            ("Francia", "33", lambda n: n.startswith("33")),
            ("Alemania", "49", lambda n: n.startswith("49")),
        ]

        # 3️⃣ Detección
        for pais, cod, regla in paises:
            if regla(num):
                return {
                    "pais": pais,
                    "codigo_pais": cod,
                    "numero_e164": f"+{num}"
                }

        # 4️⃣ Fallback
        return {
            "pais": "Desconocido",
            "codigo_pais": None,
            "numero_e164": f"+{num}" if num else None
        }
    # Expandir la información de país/número en un DataFrame temporal
    phone_info = df["NUMERO_E"].apply(detectar_pais_telefono).apply(pd.Series)

    df_transformed = pd.DataFrame({
        "codigo_estudiante": df["CODIGO_E"].astype(str),
        "nombres": df["NOMBRES_E"].astype(str).str.strip().str.title(),
        "apellidos": df["APELLIDOS_E"].astype(str).str.strip().str.title(),
        "correo": df["CORREO_E"].astype(str).str.strip().str.lower(),
        "numero": df["NUMERO_E"].astype(str).str.strip(),
        # expand country info from phone
        "pais": phone_info["pais"],
        "genero": df["GÉNERO_E"].astype(str),
        "red_contacto": df["RED DE CONTACTO_E"].astype(str),
        "nivel_educacion": df["GRADO DE INSTRUCCIÓN_E"].astype(str)
    })
    logger.info(
        f"Registros transformados correctamente: {len(df_transformed)}"
    )

    # =========================
    # SAMPLE DE DATOS
    # =========================
    logger.info("Sample de registros transformados:")
    logger.info(
        "\n" + df_transformed.head(15).to_string(index=False)
    )

    return df_transformed


__all__ = [
    "transform_cursos",
    "transform_matriculas",
    "transform_pagos_primera_cuota",
    "transform_regular_pagos",
    "transform_estudiantes",
]