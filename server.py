import os
import datetime
from flask import Flask, request
import gspread
from google.oauth2 import service_account

# =========================
# CONFIGURACIÓN GOOGLE SHEETS
# =========================

SHEET_ID = "1aBpnoIGmsWtiA6k8nKMYEs9lj0Q1tAonrimxjb6rvJg"  # tu archivo actual

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# IMPORTANTE: este archivo debe estar en la raíz del repo
SERVICE_ACCOUNT_FILE = "credenciales.json"

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
gc = gspread.authorize(creds)

# =========================
# HELPERS DE TEXTO (IGUAL QUE EN COLAB)
# =========================

def _N(s):
    return "" if s is None else str(s).strip()

def _U(s):
    return (
        _N(s)
        .upper()
        .replace("Á", "A")
        .replace("É", "E")
        .replace("Í", "I")
        .replace("Ó", "O")
        .replace("Ú", "U")
        .replace("Ü", "U")
    )

def _K(s):
    return "".join(ch for ch in _U(s) if ch.isalnum())

def _find_col(headers, candidates):
    H = [_K(h) for h in headers]
    C = [_K(c) for c in candidates]
    for idx, hk in enumerate(H):
        if hk in C:
            return idx
    return -1

def _info_destino(hdr_origen, ws_destino, write_limit):
    HDR_ORIGEN_DEST = 4  # fila 4 como en tu código
    hdrD = ws_destino.row_values(HDR_ORIGEN_DEST)
    width_full = len(hdrD)
    width_to_write = min(write_limit, width_full) if write_limit else width_full

    mapD2O = []
    for c in range(width_to_write):
        hDest = hdrD[c] if c < len(hdrD) else ""
        idxO = hdr_origen.index(hDest) if hDest in hdr_origen else -1
        if idxO == -1:
            keyD = _K(hDest)
            for j, ho in enumerate(hdr_origen):
                if _K(ho) == keyD:
                    idxO = j
                    break
        mapD2O.append(idxO)

    idxEstado     = _find_col(hdrD, ["ESTADO"])
    idxFechaReg   = _find_col(hdrD, ["FECHA REGISTRO", "FECHAREGISTRO"])
    idxOrigenCons = _find_col(hdrD, ["ORIGEN"])

    return {
        "hdrD": hdrD,
        "width_full": width_full,
        "width_to_write": width_to_write,
        "mapD2O": mapD2O,
        "idxEstado": idxEstado,
        "idxFechaReg": idxFechaReg,
        "idxOrigenCons": idxOrigenCons,
    }

# =========================
# FUNCIÓN PRINCIPAL (PEGAMOS TU LÓGICA)
# =========================

def distribuir_pedidos():
    """
    Versión adaptada de tu distribuir_pedidos() de Colab,
    usando el mismo archivo y hojas.
    """

    HDR_ORIGEN_DEST = 4
    HDR_MAQ = 1
    PARTIDAS_WRITE_LIMIT = 20
    CONS_WRITE_LIMIT = None

    sh = gc.open_by_key(SHEET_ID)

    shO   = sh.worksheet("PEDIDO_CRUDO_DIARIO")
    shM   = sh.worksheet("MAQUINA")
    shCal = sh.worksheet("PARTIDAS DESPACHO CALDEROS")
    shSta = sh.worksheet("PARTIDAS DESPACHO STA CLARA")
    shSer = sh.worksheet("PARTIDAS DESPACHO SERVICIO")
    shCons= sh.worksheet("CONSOLIDADO")

    all_origen = shO.get_all_values()
    if len(all_origen) < HDR_ORIGEN_DEST:
        return "No hay datos en PEDIDO_CRUDO_DIARIO."

    hdrO   = all_origen[HDR_ORIGEN_DEST - 1]
    datosO = all_origen[HDR_ORIGEN_DEST:]

    idxMaqGantt = _find_col(hdrO, ["MAQ-GANTT","MAQ GANTT","MAQGANTT"])
    idxLote     = _find_col(hdrO, ["LOTE"])

    if idxMaqGantt == -1:
        raise RuntimeError("No encuentro MAQ-GANTT en PEDIDO_CRUDO_DIARIO.")
    if idxLote == -1:
        raise RuntimeError("No encuentro LOTE en PEDIDO_CRUDO_DIARIO.")

    all_maq = shM.get_all_values()
    if len(all_maq) < HDR_MAQ:
        raise RuntimeError("Hoja MAQUINA sin datos.")

    hdrM   = all_maq[HDR_MAQ - 1]
    datosM = all_maq[HDR_MAQ:]

    idxMaq = _find_col(hdrM, ["MAQUINA"])
    idxUbi = _find_col(hdrM, ["UBICACION","SEDE"])

    if idxMaq == -1 or idxUbi == -1:
        raise RuntimeError("No encuentro MAQUINA/UBICACION en hoja MAQUINA.")

    mapMaqZona = {}
    for r in datosM:
        if not r or len(r) <= max(idxMaq, idxUbi):
            continue
        maqKey = _K(r[idxMaq])
        ubi    = _U(r[idxUbi])
        if not maqKey:
            continue
        zona = "SERVICIO"
        if "CALDEROS" in ubi:
            zona = "CALDEROS"
        elif "STA CLARA" in ubi or "SANTA CLARA" in ubi:
            zona = "STA CLARA"
        mapMaqZona[maqKey] = zona

    iCal = _info_destino(hdrO, shCal, PARTIDAS_WRITE_LIMIT)
    iSta = _info_destino(hdrO, shSta, PARTIDAS_WRITE_LIMIT)
    iSer = _info_destino(hdrO, shSer, PARTIDAS_WRITE_LIMIT)
    iCon = _info_destino(hdrO, shCons, CONS_WRITE_LIMIT)

    hoy = datetime.datetime.now().strftime("%d/%m/%Y")

    def es_serv_tenido(txt):
        t = _U(txt)
        return "SERVICIO" in t and "TENIDO" in t

    bufCal, bufSta, bufSer, bufCon = [], [], [], []
    filas_para_borrar = []

    def build_row_for(info, fila_origen):
        out = ["" for _ in range(info["width_to_write"])]
        for j in range(info["width_to_write"]):
            idxO = info["mapD2O"][j]
            out[j] = fila_origen[idxO] if (idxO != -1 and idxO < len(fila_origen)) else ""
        return out

    for i, fila in enumerate(datosO):
        fila_hoja = HDR_ORIGEN_DEST + 1 + i
        if not any(_N(c) for c in fila):
            continue

        maqVal = _N(fila[idxMaqGantt]) if idxMaqGantt < len(fila) else ""
        maqKey = _K(maqVal)
        lote   = _N(fila[idxLote]) if idxLote < len(fila) else ""

        zona = mapMaqZona.get(maqKey, "SERVICIO")

        if zona == "SERVICIO" and es_serv_tenido(lote):
            continue

        if zona == "CALDEROS":
            infoD, target = iCal, bufCal
        elif zona == "STA CLARA":
            infoD, target = iSta, bufSta
        else:
            infoD, target = iSer, bufSer

        rowDest = build_row_for(infoD, fila)
        if 0 <= infoD["idxFechaReg"] < infoD["width_to_write"]:
            rowDest[infoD["idxFechaReg"]] = hoy
        if 0 <= infoD["idxEstado"] < infoD["width_to_write"]:
            rowDest[infoD["idxEstado"]] = "VACIO"

        target.append(rowDest)
        filas_para_borrar.append(fila_hoja)

        rowCon = ["" for _ in range(iCon["width_to_write"])]
        for j in range(iCon["width_to_write"]):
            idxO = iCon["mapD2O"][j]
            rowCon[j] = fila[idxO] if (idxO != -1 and idxO < len(fila)) else ""
        if 0 <= iCon["idxFechaReg"] < iCon["width_to_write"]:
            rowCon[iCon["idxFechaReg"]] = hoy
        if 0 <= iCon["idxOrigenCons"] < iCon["width_to_write"]:
            rowCon[iCon["idxOrigenCons"]] = zona

        bufCon.append(rowCon)

    # Escritura segura (extiende filas si hace falta)
    def append_rows(ws, rows, width):
        if not rows:
            return
        existing = ws.get_all_values()
        current_rows = len(existing)
        start_row = current_rows + 1
        needed_rows = start_row + len(rows) - 1
        if needed_rows > current_rows:
            ws.add_rows(needed_rows - current_rows)
        ws.update(f"A{start_row}", rows)

    append_rows(shCal,  bufCal, iCal["width_to_write"])
    append_rows(shSta,  bufSta, iSta["width_to_write"])
    append_rows(shSer,  bufSer, iSer["width_to_write"])
    append_rows(shCons, bufCon, iCon["width_to_write"])

    if filas_para_borrar:
        for r in sorted(filas_para_borrar, reverse=True):
            shO.delete_rows(r)

    mensaje = (
        f"✔ Distribución completada.\n"
        f"   • Calderos: {len(bufCal)}\n"
        f"   • Sta Clara: {len(bufSta)}\n"
        f"   • Servicio: {len(bufSer)}\n"
        f"   • Consolidados: {len(bufCon)}"
    )
    print(mensaje)
    return mensaje

# =========================
# FLASK APP
# =========================

app = Flask(__name__)

@app.route("/ping")
def ping():
    return "OK", 200

@app.route("/distribuir", methods=["GET"])
def endpoint_distribuir():
    try:
        msg = distribuir_pedidos()
        return msg, 200
    except Exception as e:
        return f"✖ Error en distribuir_pedidos: {e}", 500

# Punto de entrada para Render
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
