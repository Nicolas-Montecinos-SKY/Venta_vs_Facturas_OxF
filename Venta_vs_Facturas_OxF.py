# %% [markdown]
# # Conciliación

# %% [markdown]
# ## Código

# %%
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

import numpy as np
import pandas as pd


# ============================================================
# 0) Config
# ============================================================
@dataclass(frozen=True)
class Config:
    sheet_ra: str = "SABRE"
    sheet_oxf: str = "Base"
    suf_ra: str = "_Venta_RA_OxF"
    suf_oxf: str = "_Facturacion_AR_OxF"

    patron_fraude: str = r"FRAUDE"
    patron_test: str = r"TEST"

    tolerancia_monto: float = 0.01
    tc_from_currency: str = "USD"
    tc_conversion_type: str = "Corporate"


CFG = Config()


# ============================================================
# 1) Utilidades
# ============================================================
def require_columns(df: pd.DataFrame, required: list[str], df_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"[{df_name}] Faltan columnas requeridas: {missing}")


def coalesce(s1: pd.Series | None, s2: pd.Series | None) -> pd.Series:
    if s1 is None and s2 is None:
        return pd.Series(dtype="object")
    if s1 is None:
        return s2
    if s2 is None:
        return s1
    return s1.combine_first(s2)


def add_suffix_except(df: pd.DataFrame, suffix: str, keep: tuple[str, ...] = ("ID",)) -> pd.DataFrame:
    rename_map = {c: f"{c}{suffix}" for c in df.columns if c not in keep}
    return df.rename(columns=rename_map)


# ============================================================
# 2) Selectores de archivos
# ============================================================
def _tk_pick(fn, **kwargs) -> str:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    out = fn(**kwargs)
    root.destroy()
    if not out:
        raise RuntimeError("Selección cancelada.")
    return out


def seleccionar_carpeta(titulo: str) -> str:
    return _tk_pick(filedialog.askdirectory, title=titulo)


def seleccionar_archivo_excel(titulo: str) -> str:
    return _tk_pick(
        filedialog.askopenfilename,
        title=titulo,
        filetypes=(("Excel files", "*.xlsx *.xlsm *.xls"),),
    )


def seleccionar_archivo_tc(titulo: str) -> str:
    return _tk_pick(
        filedialog.askopenfilename,
        title=titulo,
        filetypes=(("TC files", "*.xlsx *.xlsm *.xls *.csv"),),
    )


# ============================================================
# 3) Tipos de Cambio (TC)
# ============================================================
def cargar_tabla_tc(path_tc: str, cfg: Config) -> pd.DataFrame:
    p = Path(path_tc)

    if p.suffix.lower() == ".csv":
        tc = pd.read_csv(p, header=1, sep=None, engine="python")
    else:
        tc = pd.read_excel(p, header=1)  # encabezados en fila 2

    required = ["DATE", "FROM_CURRENCY", "TO_CURRENCY", "CONVERSION_TYPE", "CONVERSION_RATE"]
    require_columns(tc, required, "TC")

    tc = tc.copy()
    tc["DATE"] = pd.to_datetime(tc["DATE"], dayfirst=True, errors="coerce").dt.date
    tc["FROM_CURRENCY"] = tc["FROM_CURRENCY"].astype(str).str.strip().str.upper()
    tc["TO_CURRENCY"] = tc["TO_CURRENCY"].astype(str).str.strip().str.upper()
    tc["CONVERSION_TYPE"] = tc["CONVERSION_TYPE"].astype(str).str.strip()
    tc["CONVERSION_RATE"] = pd.to_numeric(tc["CONVERSION_RATE"], errors="coerce")

    tc = tc[
        (tc["FROM_CURRENCY"] == cfg.tc_from_currency)
        & (tc["CONVERSION_TYPE"].str.upper() == cfg.tc_conversion_type.upper())
    ].copy()

    tc = tc.dropna(subset=["DATE", "TO_CURRENCY", "CONVERSION_RATE"])
    tc = tc.sort_values(["DATE", "TO_CURRENCY"]).drop_duplicates(["DATE", "TO_CURRENCY"], keep="last")

    return tc


# ============================================================
# 4) Consolidación RA (SABRE)
# ============================================================
def consolidar_excels_en_df(
    carpeta: str,
    sheet_name: str,
    extensiones: tuple[str, ...] = (".xlsx", ".xlsm", ".xls"),
    dtype: dict | None = None,
) -> pd.DataFrame:
    carpeta_path = Path(carpeta)
    archivos = [p for p in carpeta_path.rglob("*") if p.suffix.lower() in extensiones and "~$" not in p.name]
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos Excel en: {carpeta}")

    dfs: list[pd.DataFrame] = []
    errores: list[tuple[str, str]] = []

    for f in archivos:
        try:
            df_tmp = pd.read_excel(f, sheet_name=sheet_name, dtype=dtype)
            df_tmp["source_file"] = f.name
            df_tmp["source_path"] = str(f)
            dfs.append(df_tmp)
        except Exception as e:
            errores.append((str(f), str(e)))

    if not dfs:
        raise RuntimeError(
            "No se pudo leer ningún archivo correctamente.\n"
            + "\n".join([f"{a} -> {err}" for a, err in errores[:10]])
        )

    df_consolidado = pd.concat(dfs, ignore_index=True, sort=False)

    print(f"✅ Archivos encontrados: {len(archivos)}")
    print(f"✅ Archivos leídos OK:  {len(dfs)}")
    print(f"⚠️ Archivos con error:  {len(errores)}")
    if errores:
        print("\nPrimeros errores (máx 5):")
        for a, err in errores[:5]:
            print(" -", a, "->", err)

    return df_consolidado


# ============================================================
# 5) Preparación RA / OxF
# ============================================================
def prepare_ra(df_ra_raw: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "pnr", "document_nbr", "issue_date",
        "method_of_payment", "original_amount", "original_currency", "usd_amount",
        "station_name", "Agente", "accounting_country CORREGIDO",
    ]
    require_columns(df_ra_raw, cols, "RA")

    ra = df_ra_raw[cols].copy()

    # Filtros negocio
    ra = ra[ra["method_of_payment"].astype(str).str.strip().str.upper().ne("IN")].copy()
    ra = ra[ra["station_name"].astype(str).str.strip().str.upper().ne("TRAVELFUSION")].copy()

    # ID
    ra["ID"] = ra["pnr"].astype(str).str.strip() + "_" + ra["document_nbr"].astype(str).str.strip()

    # Normalizaciones
    ra["original_currency"] = (
        ra["original_currency"].astype(str)
          .str.replace(r"[^A-Za-z]", "", regex=True)
          .str.upper()
          .replace({"NAN": np.nan})
    )

    ra["original_amount"] = pd.to_numeric(ra["original_amount"], errors="coerce")
    ra["usd_amount"] = pd.to_numeric(ra["usd_amount"], errors="coerce")
    ra["issue_date"] = pd.to_datetime(ra["issue_date"], errors="coerce")

    return ra


def prepare_oxf(df_oxf_raw: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    cols = [
        "Reserva", "N_de_Boleto", "Pasajero", "Monto_ASR", "RUT_RUC", "Folio",
        "Comentario real", "Fecha consolidada", "Folio manual", "Status Emisión",
        "Moneda", "Razon_Social", "Fecha_de_Venta",
    ]
    require_columns(df_oxf_raw, cols, "OxF")

    tmp = df_oxf_raw[cols].copy()

    # ID (✅ faltaba en tu script)
    tmp["ID"] = tmp["Reserva"].astype(str).str.strip() + "_" + tmp["N_de_Boleto"].astype(str).str.strip()

    # Folios (✅ faltaba crear Folio_final)
    tmp["Folio"] = pd.to_numeric(tmp["Folio"], errors="coerce").astype("Int64")
    tmp["Folio manual"] = pd.to_numeric(tmp["Folio manual"], errors="coerce").astype("Int64")
    tmp["Folio_final"] = tmp["Folio manual"].combine_first(tmp["Folio"]).astype("Int64")

    # Monto numérico
    tmp["Monto_ASR"] = pd.to_numeric(tmp["Monto_ASR"], errors="coerce")

    # Fechas a datetime
    tmp["Fecha consolidada"] = pd.to_datetime(tmp["Fecha consolidada"], errors="coerce")
    tmp["Fecha_de_Venta"] = pd.to_datetime(tmp["Fecha_de_Venta"], errors="coerce")

    # Flags FRAUDE / TEST
    coment = tmp["Comentario real"].astype(str)
    tmp["es_fraude"] = coment.str.contains(cfg.patron_fraude, case=False, na=False)
    tmp["es_test"] = coment.str.contains(cfg.patron_test, case=False, na=False)

    tmp["flag_comentario"] = np.select(
        [tmp["es_fraude"] & tmp["es_test"], tmp["es_fraude"], tmp["es_test"]],
        ["FRAUDE|TEST", "FRAUDE", "TEST"],
        default=""
    )

    # Agregar por ID
    oxf = (
        tmp.groupby("ID", as_index=False)
           .agg({
               "Reserva": "first",
               "N_de_Boleto": "first",
               "Pasajero": "first",
               "Monto_ASR": "sum",
               "Moneda": "first",
               "RUT_RUC": "first",
               "Razon_Social": "first",
               "Folio_final": "first",
               "Fecha consolidada": "first",
               "Status Emisión": "first",
               "Fecha_de_Venta": "first",
               "es_fraude": "max",
               "es_test": "max",
               "flag_comentario": "first",
           })
           .rename(columns={"N_de_Boleto": "document_nbr"})
    )

    return oxf


# ============================================================
# 6) Merge + Conciliación
# ============================================================
def merge_sources(ra: pd.DataFrame, oxf: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    ra_ren = add_suffix_except(ra, cfg.suf_ra, keep=("ID",))
    oxf_ren = add_suffix_except(oxf, cfg.suf_oxf, keep=("ID",))

    df = ra_ren.merge(oxf_ren, on="ID", how="outer", indicator=True)

    df["origen"] = df["_merge"].map({
        "left_only":  "Venta sin identificar",
        "right_only": "Factura sin identificar",
        "both":       "Match Venta-Factura",
    })

    orden_merge = pd.Categorical(df["_merge"], categories=["both", "left_only", "right_only"], ordered=True)
    df = (
        df.assign(_merge_orden=orden_merge)
          .sort_values(["_merge_orden", "ID"])
          .drop(columns=["_merge_orden"])
          .reset_index(drop=True)
    )
    return df


def add_conciliation(df_union: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = df_union.copy()

    col_ra = f"original_amount{cfg.suf_ra}"
    col_oxf = f"Monto_ASR{cfg.suf_oxf}"

    df[col_ra] = pd.to_numeric(df.get(col_ra), errors="coerce")
    df[col_oxf] = pd.to_numeric(df.get(col_oxf), errors="coerce")

    df["diff_monto"] = df[col_ra] - df[col_oxf]
    df["Conciliado"] = pd.Series(pd.NA, index=df.index, dtype="string")

    mask_both = df["_merge"].eq("both")
    mask_montos_ok = mask_both & df[col_ra].notna() & df[col_oxf].notna()
    mask_ok = mask_montos_ok & (df["diff_monto"].abs() <= cfg.tolerancia_monto)

    # faltantes dentro de both
    mask_ambos_na = mask_both & df[col_ra].isna() & df[col_oxf].isna()
    df.loc[mask_ambos_na, "Conciliado"] = "FALTA TICKET EN AMBAS FUENTES"
    df.loc[mask_both & df[col_ra].isna() & ~mask_ambos_na, "Conciliado"] = "FALTA TICKET EN RA"
    df.loc[mask_both & df[col_oxf].isna() & ~mask_ambos_na, "Conciliado"] = "FALTA TICKET EN OxF"

    df.loc[mask_ok, "Conciliado"] = "CUADRADO"
    df.loc[mask_montos_ok & ~mask_ok, "Conciliado"] = "NO CUADRADO"
    df.loc[~mask_both, "Conciliado"] = "SIN MATCH"

    return df


# ============================================================
# 7) Columnas consolidadas
# ============================================================
def unificar_columnas_reserva_ticket(df_union: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = df_union.copy()

    col_pnr_ra  = f"pnr{cfg.suf_ra}"
    col_res_oxf = f"Reserva{cfg.suf_oxf}"
    col_doc_ra  = f"document_nbr{cfg.suf_ra}"
    col_doc_oxf = f"document_nbr{cfg.suf_oxf}"

    # 🔹 Coalesce (prioriza RA, si no existe usa OxF)
    reserva = coalesce(df.get(col_pnr_ra), df.get(col_res_oxf))
    ticket  = coalesce(df.get(col_doc_ra), df.get(col_doc_oxf))

    # 🔹 Limpieza segura de strings (Series → .str.strip())
    df["Código de Reserva"] = reserva.astype(str).str.strip()
    df["Número de Ticket"]  = ticket.astype(str).str.strip()

    # 🔹 Normalizar vacíos
    df["Código de Reserva"] = df["Código de Reserva"].replace(
        {"": pd.NA, "nan": pd.NA, "None": pd.NA}
    )
    df["Número de Ticket"] = df["Número de Ticket"].replace(
        {"": pd.NA, "nan": pd.NA, "None": pd.NA}
    )

    # 🔹 Eliminar columnas originales RA / OxF
    drop_cols = [
        c for c in [col_pnr_ra, col_res_oxf, col_doc_ra, col_doc_oxf]
        if c in df.columns
    ]

    return df.drop(columns=drop_cols)



def normalizar_status_emision(df_union: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    """
    Crea columna NUEVA: Status Emisión_NORMALIZADO_Facturacion_AR_OxF
    (No pisa el Status original)
    """
    df = df_union.copy()

    col_status = f"Status Emisión{cfg.suf_oxf}"
    col_out    = f"Status Emisión_NORMALIZADO{cfg.suf_oxf}"
    col_fraude = f"es_fraude{cfg.suf_oxf}"
    col_test   = f"es_test{cfg.suf_oxf}"

    if col_status not in df.columns:
        return df

    s = df[col_status].astype(str).str.strip()

    fraude = df.get(col_fraude)
    test = df.get(col_test)

    fraude = (fraude.fillna(False).astype(bool) if fraude is not None else pd.Series(False, index=df.index))
    test   = (test.fillna(False).astype(bool) if test is not None else pd.Series(False, index=df.index))

    df[col_out] = np.where(
        fraude & test, "FRAUDE|TEST",
        np.where(
            fraude, "FRAUDE",
            np.where(
                test, "TEST",
                np.where(
                    s.str.startswith("Facturado", na=False),
                    "Facturado",
                    np.where(
                        s.str.fullmatch("No Facturado", case=False, na=False),
                        "Pendiente a Facturar",
                        "Sin información",
                    )
                )
            )
        )
    )

    return df


def agregar_fecha_consolidada(df_union: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = df_union.copy()

    col_ra  = f"issue_date{cfg.suf_ra}"
    col_oxf = f"Fecha consolidada{cfg.suf_oxf}"

    fecha_ra = pd.to_datetime(df.get(col_ra), errors="coerce")
    fecha_oxf = pd.to_datetime(df.get(col_oxf), errors="coerce")

    df["fecha_consolidada"] = pd.concat([fecha_ra, fecha_oxf], axis=1).min(axis=1)
    return df


def agregar_usd_unificado(df_union: pd.DataFrame, tc: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    df = df_union.copy()

    col_usd_ra     = f"usd_amount{cfg.suf_ra}"
    col_monto_oxf  = f"Monto_ASR{cfg.suf_oxf}"
    col_moneda_oxf = f"Moneda{cfg.suf_oxf}"
    col_fecha_oxf  = f"Fecha consolidada{cfg.suf_oxf}"

    if col_usd_ra in df.columns:
        df[col_usd_ra] = pd.to_numeric(df[col_usd_ra], errors="coerce")
    df[col_monto_oxf] = pd.to_numeric(df.get(col_monto_oxf), errors="coerce")

    moneda = df.get(col_moneda_oxf)
    moneda = moneda.astype(str).str.strip().str.upper() if moneda is not None else pd.Series([None]*len(df), index=df.index)

    fecha = df.get(col_fecha_oxf)
    fecha = pd.to_datetime(fecha, errors="coerce").dt.date if fecha is not None else pd.Series([None]*len(df), index=df.index)

    tc2 = tc.copy()
    tc2["DATE"] = pd.to_datetime(tc2["DATE"], errors="coerce").dt.date
    tc2["TO_CURRENCY"] = tc2["TO_CURRENCY"].astype(str).str.strip().str.upper()
    tc2["CONVERSION_RATE"] = pd.to_numeric(tc2["CONVERSION_RATE"], errors="coerce")

    rate_exact = tc2.set_index(["DATE", "TO_CURRENCY"])["CONVERSION_RATE"]

    tc_last = (
        tc2.dropna(subset=["DATE", "TO_CURRENCY", "CONVERSION_RATE"])
           .sort_values(["TO_CURRENCY", "DATE"])
           .groupby("TO_CURRENCY", as_index=False)
           .tail(1)
           .set_index("TO_CURRENCY")
    )
    rate_last = tc_last["CONVERSION_RATE"]

    key = pd.MultiIndex.from_arrays([fecha, moneda])
    rates_exact = pd.Series(key.map(rate_exact), index=df.index, dtype="float64")
    rates_fallback = pd.Series(moneda.map(rate_last), index=df.index, dtype="float64")

    rates_exact = pd.Series(np.where(moneda.eq("USD"), 1.0, rates_exact), index=df.index)
    rates_fallback = pd.Series(np.where(moneda.eq("USD"), 1.0, rates_fallback), index=df.index)

    rates_final = rates_exact.combine_first(rates_fallback)

    usd_from_oxf = (df[col_monto_oxf] / rates_final).where(rates_final.notna() & (rates_final != 0))

    usd_consolidado = df.get(col_usd_ra).combine_first(usd_from_oxf)
    df["usd_amount_consolidado"] = usd_consolidado.round(2)

    df["usd_fuente"] = np.select(
        [
            df.get(col_usd_ra).notna(),
            usd_from_oxf.notna() & rates_exact.notna(),
            usd_from_oxf.notna() & rates_exact.isna(),
        ],
        [
            "RA",
            "OxF_convertido_TC_fecha",
            "OxF_convertido_TC_ultima",
        ],
        default="Sin_USD"
    )

    df["tc_usada"] = rates_final
    return df


# ============================================================
# 8) Orden final
# ============================================================
def ordenar_columnas_df_union(df_union: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    cols_orden = [
        "origen", "Conciliado", "Código de Reserva", "Número de Ticket",
        f"issue_date{cfg.suf_ra}",
        f"Fecha consolidada{cfg.suf_oxf}",
        "fecha_consolidada",

        f"method_of_payment{cfg.suf_ra}",
        f"original_amount{cfg.suf_ra}",
        f"original_currency{cfg.suf_ra}",
        f"usd_amount{cfg.suf_ra}",
        "usd_amount_consolidado",
        "usd_fuente",
        "tc_usada",

        f"station_name{cfg.suf_ra}",
        f"Agente{cfg.suf_ra}",
        f"accounting_country CORREGIDO{cfg.suf_ra}",

        f"Pasajero{cfg.suf_oxf}",
        f"Monto_ASR{cfg.suf_oxf}",
        f"Moneda{cfg.suf_oxf}",
        f"RUT_RUC{cfg.suf_oxf}",
        f"Razon_Social{cfg.suf_oxf}",
        f"Folio_final{cfg.suf_oxf}",
        f"Status Emisión{cfg.suf_oxf}",
        f"Status Emisión_NORMALIZADO{cfg.suf_oxf}",
        f"Fecha_de_Venta{cfg.suf_oxf}",

        f"es_fraude{cfg.suf_oxf}",
        f"es_test{cfg.suf_oxf}",
        f"flag_comentario{cfg.suf_oxf}",

        "diff_monto",
    ]

    faltantes = [c for c in cols_orden if c not in df_union.columns]
    if faltantes:
        raise KeyError(f"Faltan columnas para el orden final: {faltantes}")

    return df_union[cols_orden].copy()



# %% [markdown]
# ## Ejecutar

# %%
# ============================================================
# 9) Ejecución
# ============================================================
carpeta_ra = seleccionar_carpeta("Selecciona la carpeta con los Excel de RA (SABRE)")
archivo_oxf = seleccionar_archivo_excel("Selecciona el archivo Excel de OxF (Base)")
archivo_tc = seleccionar_archivo_tc("Selecciona el archivo de Tipo de Cambio (TC)")

df_RA_raw = consolidar_excels_en_df(
    carpeta_ra,
    sheet_name=CFG.sheet_ra,
    dtype={"document_nbr": str, "pnr": str},
)

df_OxF_raw = pd.read_excel(
    archivo_oxf,
    sheet_name=CFG.sheet_oxf,
    dtype={"N_de_Boleto": str, "Reserva": str},
)

tc = cargar_tabla_tc(archivo_tc, CFG)

print("RAW shapes:", df_RA_raw.shape, df_OxF_raw.shape)



# %%
ra = prepare_ra(df_RA_raw)
oxf = prepare_oxf(df_OxF_raw, CFG)

print("Prepared shapes:", ra.shape, oxf.shape)


# %%
df_union = merge_sources(ra, oxf, CFG)
df_union = add_conciliation(df_union, CFG)

df_union = unificar_columnas_reserva_ticket(df_union, CFG)
df_union = normalizar_status_emision(df_union, CFG)
df_union = agregar_usd_unificado(df_union, tc, CFG)
df_union = agregar_fecha_consolidada(df_union, CFG)

df_union = ordenar_columnas_df_union(df_union, CFG)

# %%
# Export
out_dir = Path("Archivos_csv")
out_dir.mkdir(parents=True, exist_ok=True)

base_name = "Venta_RA_vs_Facturacion_OxF"

file_consolidado = out_dir / f"{base_name}_CONSOLIDADO.csv"
df_union.to_csv(file_consolidado, index=False, encoding="utf-8-sig")
print(f"✅ CSV exportado OK: {file_consolidado} | Filas: {len(df_union):,}")

df_solo_venta = df_union[df_union["origen"] == "Venta sin identificar"].copy()
file_venta = out_dir / f"{base_name}_Ventas_no_reconocidas.csv"
df_solo_venta.to_csv(file_venta, index=False, encoding="utf-8-sig")
print(f"✅ CSV exportado OK: {file_venta} | Filas: {len(df_solo_venta):,}")

df_solo_oxf = df_union[df_union["origen"] == "Factura sin identificar"].copy()
file_oxf = out_dir / f"{base_name}_Facturas_no_reconocidas.csv"
df_solo_oxf.to_csv(file_oxf, index=False, encoding="utf-8-sig")
print(f"✅ CSV exportado OK: {file_oxf} | Filas: {len(df_solo_oxf):,}")

# %%
df_union.head()


