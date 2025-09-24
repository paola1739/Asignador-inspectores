#!/usr/bin/env python3
# asignar_comisarios.py
from arcgis.gis import GIS
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import pprint
import traceback

DEBUG = True

print("🟡 Script asignar_comisario iniciado...")

def find_col(cols, candidates):
    """Buscar la primera columna en cols que coincida con cualquiera de candidates (case-insensitive)."""
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None

def safe_get(obj, key, default=None):
    """Obtener valor con tolerancia si obj es Series/dict."""
    try:
        if key is None:
            return default
        # pandas Series supports .get
        if hasattr(obj, "get"):
            val = obj.get(key, default)
            if val is not None:
                return val
        # fallback item access
        return obj[key]
    except Exception:
        try:
            return obj.get(key, default)
        except Exception:
            return default

def extract_geometry_from_row(row):
    """Intenta extraer x,y de varias posibles representaciones en la row."""
    candidates = ['SHAPE', 'shape', 'geometry', 'geom', 'SHAPE@XY', 'Shape']
    for c in candidates:
        val = safe_get(row, c, None)
        if val is None:
            continue
        # si ya es dict con x,y
        if isinstance(val, dict):
            x = val.get("x") or val.get("X")
            y = val.get("y") or val.get("Y")
            if x is not None and y is not None:
                return {"x": float(x), "y": float(y), "spatialReference": {"wkid": 4326}}
            # si viene como coordinates [lon, lat]
            coords = val.get("coordinates") if "coordinates" in val else None
            if coords and isinstance(coords, (list, tuple)) and len(coords) >= 2:
                return {"x": float(coords[0]), "y": float(coords[1]), "spatialReference": {"wkid": 4326}}
        # si es un objeto con atributos x,y
        if hasattr(val, "x") and hasattr(val, "y"):
            try:
                return {"x": float(val.x), "y": float(val.y), "spatialReference": {"wkid": 4326}}
            except Exception:
                pass
        # si es tupla/lista (x,y)
        if isinstance(val, (list, tuple)) and len(val) >= 2:
            try:
                return {"x": float(val[0]), "y": float(val[1]), "spatialReference": {"wkid": 4326}}
            except Exception:
                pass
    return None

def comprobar_serializable(lista):
    """Intenta serializar con json.dumps usando default=str. Si falla, devuelve False y el error."""
    try:
        json.dumps(lista, default=str)
        return True, None
    except Exception as e:
        return False, e

def ejecutar_asignacion_comisario():
    usuario = os.getenv("AGOL_USERNAME")
    clave = os.getenv("AGOL_PASSWORD")
    if not usuario or not clave:
        print("❌ No se encontraron credenciales en las variables de entorno (AGOL_USERNAME/AGOL_PASSWORD).")
        return

    try:
        gis = GIS("https://www.arcgis.com", usuario, clave)
        print(f"🟢 Sesión iniciada como: {gis.users.me.username}")
    except Exception as e:
        print(f"❌ Error al iniciar sesión en ArcGIS Online: {e}")
        return

    try:
        # IDs que usas (mantener los ids que tienes)
        item_tabla = gis.content.get("aa7cb6814d7d44beaa2557533103e7aa")
        item_denuncia = gis.content.get("60c69b82ab074b65a8a239fcd2067ce4")
        item_workforce = gis.content.get("bf86d367917747cf82fb57a9128eed0e")

        tabla_comisarios = item_tabla.tables[0]
        layer_denuncias = item_denuncia.layers[0]
        layer_asignaciones = item_workforce.layers[0]
        layer_workers = item_workforce.layers[1]

        features_comisarios = tabla_comisarios.query(where="1=1", out_fields="*", return_geometry=False)
        features_denuncias = layer_denuncias.query(
            where="estado_tramite = 'Supervision Finalizada' AND proceso_administrativo = 'Si'",
            out_fields="*",
            return_geometry=True
        )
        features_workers = layer_workers.query(where="1=1", out_fields="*", return_geometry=False)

        df_comisarios = features_comisarios.sdf
        df_denuncias = features_denuncias.sdf
        df_workers = features_workers.sdf

        print(f"Total de denuncias para comisarios encontradas: {len(df_denuncias)}")
        if DEBUG:
            print("Columnas tabla comisarios:", list(df_comisarios.columns))
            print("Columnas capa denuncias:", list(df_denuncias.columns))
            print("Columnas workers:", list(df_workers.columns))

        # localizar columnas relevantes (tolerante)
        col_siglas = find_col(df_comisarios.columns, ["siglas", "siglas_inspector", "sigla"])
        col_nombre = find_col(df_comisarios.columns, ["nombre", "name"])
        col_user = find_col(df_comisarios.columns, ["nomre_de_usuario", "username", "userid", "usuario"])
        col_obj_comisario = find_col(df_comisarios.columns, ["objectid", "OBJECTID", "oid", "object_id"])
        col_num_tramites = find_col(df_comisarios.columns, ["num_tramites", "numtramites", "num_tramite", "num_trámites"])
        col_ultimo_num = find_col(df_comisarios.columns, ["ultimo_numero", "ultimo_num", "ultimo"])

        col_obj_denuncia = find_col(df_denuncias.columns, ["objectid", "OBJECTID", "oid", "object_id"])
        col_globalid_denuncia = find_col(df_denuncias.columns, ["globalid", "GlobalID", "GLOBALID"])
        col_siglas_area_den = find_col(df_denuncias.columns, ["siglas_area", "siglas"])

        worker_user_col = find_col(df_workers.columns, ["userid", "username", "user", "usuario"])

        if DEBUG:
            print("Mappings -> siglas:", col_siglas, "nombre:", col_nombre, "user:", col_user,
                  "obj_comisario:", col_obj_comisario, "num_tramites:", col_num_tramites, "ultimo_num:", col_ultimo_num)
            print("Denuncia obj:", col_obj_denuncia, "denuncia globalid:", col_globalid_denuncia, "siglas_area:", col_siglas_area_den)
            print("Worker user col:", worker_user_col)

        if df_denuncias.empty:
            print("No hay denuncias para procesar.")
            return

        denuncias_actualizadas = []
        comisarios_actualizados = []
        tareas_creadas = []

        # GUID del tipo de asignación "Comisario"
        assignmenttype_guid = "33aec22e-5094-4cce-9493-a3444d8fba8c"

        for idx, row in df_denuncias.iterrows():
            try:
                if df_comisarios.empty:
                    print("❌ No hay comisarios disponibles en la tabla")
                    continue

                # ordenar por num_tramites (si no existe, no ordenar)
                if col_num_tramites:
                    sorted_df = df_comisarios.sort_values(by=col_num_tramites, ascending=True, na_position='last')
                else:
                    sorted_df = df_comisarios

                comisario_asignado = sorted_df.iloc[0]

                # obtener valores con tolerancia
                nombre_comisario = safe_get(comisario_asignado, col_nombre, "SinNombre")
                siglas_comisario = safe_get(comisario_asignado, col_siglas, "XX")
                username_comisario = safe_get(comisario_asignado, col_user, None)
                ultimo_numero_val = safe_get(comisario_asignado, col_ultimo_num, 0)
                try:
                    ultimo_numero_val = int(ultimo_numero_val) if ultimo_numero_val is not None else 0
                except Exception:
                    ultimo_numero_val = 0

                ultimo_numero = ultimo_numero_val + 1

                anio_actual = datetime.utcnow().year
                siglas_area = safe_get(row, col_siglas_area_den, "XX")
                numero_formulario = f"DGSH-CO-{siglas_comisario}-{anio_actual}-{ultimo_numero}"

                # preparar actualización denuncia (usar el nombre correcto del campo objectid)
                objid_den_value = safe_get(row, col_obj_denuncia, None)
                globalid_den_value = safe_get(row, col_globalid_denuncia, None)

                attrs_denuncia = {}
                if col_obj_denuncia:
                    attrs_denuncia[col_obj_denuncia] = objid_den_value
                else:
                    # fallback a 'objectid' por compatibilidad si no lo encontramos
                    attrs_denuncia["objectid"] = objid_den_value

                attrs_denuncia.update({
                    "comisario_asignado": nombre_comisario,
                    "estado_tramite": "Asignado a comisario",
                    "id_denuncia_comparar_comisario": str(globalid_den_value) if globalid_den_value else None
                })
                denuncias_actualizadas.append({"attributes": attrs_denuncia})

                # preparar actualización tabla comisarios (usar su objectid real)
                comisario_obj_val = safe_get(comisario_asignado, col_obj_comisario, None)
                attrs_comisario = {}
                if col_obj_comisario:
                    attrs_comisario[col_obj_comisario] = comisario_obj_val
                else:
                    attrs_comisario["objectid"] = comisario_obj_val

                # actualizar num_tramites y ultimo_numero si el campo existe, si no añadir como claves estándar
                if col_num_tramites:
                    try:
                        cur_num = int(safe_get(comisario_asignado, col_num_tramites, 0))
                    except Exception:
                        cur_num = 0
                    attrs_comisario[col_num_tramites] = cur_num + 1
                else:
                    attrs_comisario["num_tramites"] = 1

                if col_ultimo_num:
                    attrs_comisario[col_ultimo_num] = ultimo_numero
                else:
                    attrs_comisario["ultimo_numero"] = ultimo_numero

                comisarios_actualizados.append({"attributes": attrs_comisario})

                # buscar worker en Workforce por userid/username
                if username_comisario is None:
                    print(f"⚠️ No hay username para comisario {nombre_comisario}; se omite creación de tarea para esta denuncia.")
                    continue

                # localizar index del worker en df_workers comparando con worker_user_col
                if worker_user_col:
                    matches = df_workers[df_workers[worker_user_col] == username_comisario]
                else:
                    # intentar con varias columnas comunes
                    alt_col = find_col(df_workers.columns, ["username", "userid", "user", "usuario"])
                    if alt_col:
                        matches = df_workers[df_workers[alt_col] == username_comisario]
                    else:
                        matches = pd.DataFrame()

                if matches.empty:
                    print(f"No se encontró al trabajador '{username_comisario}' en Workforce")
                    continue

                # tomar el primer match y obtener su GlobalID desde features_workers (coincidir index)
                worker_idx = matches.index[0]
                try:
                    worker_feature = features_workers.features[worker_idx]
                    worker_globalid = worker_feature.attributes.get("GlobalID") or worker_feature.attributes.get("globalid")
                except Exception:
                    # fallback: intentar obtener 'GlobalID' desde el dataframe (si existe)
                    worker_globalid = safe_get(matches.iloc[0], "GlobalID", safe_get(matches.iloc[0], "globalid", None))

                if not worker_globalid:
                    print(f"⚠️ Worker encontrado pero no tiene GlobalID: {username_comisario}")
                    continue

                # geometría
                geometry = extract_geometry_from_row(row)

                descripcion_tarea = (
                    f"Informe de supervisión finalizada\n"
                    f"Infractor: {safe_get(row, 'cedula_infractor','')}\n"
                    f"Proceso administrativo: {safe_get(row, 'proceso_administrativo','')}\n"
                )

                due_date_iso = (datetime.utcnow() + timedelta(days=3)).isoformat() + "Z"
                assigned_date_iso = datetime.utcnow().isoformat() + "Z"

                tarea_attributes = {
                    "description": descripcion_tarea,
                    "status": 1,
                    "priority": 0,
                    "assignmenttype": assignmenttype_guid,
                    "location": safe_get(row, "direccion", ""),
                    "workorderid": str(globalid_den_value) if globalid_den_value else (str(objid_den_value) if objid_den_value else ""),
                    "codigoformulario": numero_formulario,
                    "nombrecomisario": nombre_comisario,
                    "workerid": worker_globalid,
                    # fechas como ISO strings (evitamos objetos datetime crudos para prevenir problemas)
                    "duedate": due_date_iso,
                    "assigneddate": assigned_date_iso
                }

                tarea_dict = {"attributes": tarea_attributes, "geometry": geometry}
                tareas_creadas.append(tarea_dict)

            except Exception as e_row:
                print("❌ Error procesando denuncia index", idx)
                traceback.print_exc()
                continue

        # Debug: ver ejemplo y comprobar serialización
        if DEBUG:
            print("\n--- Ejemplo tarea creada (primer elemento) ---")
            if tareas_creadas:
                pprint.pprint(tareas_creadas[0])
            else:
                print("No hay tareas en tareas_creadas.")

        ok, err = comprobar_serializable(tareas_creadas)
        if not ok:
            print("❌ Error: tareas_creadas NO serializable por JSON. Excepción:", err)
            # mostrar tipos y repr de primeros 5 elementos para depuración
            for i, item in enumerate(tareas_creadas[:5]):
                print(f"Item {i} type: {type(item)}")
                try:
                    print(json.dumps(item, default=str, ensure_ascii=False))
                except Exception as e_item:
                    print(" -> sigue sin serializarse:", e_item)
            return

        # Guardar tareas (adds)
        try:
            if tareas_creadas:
                resp_tareas = layer_asignaciones.edit_features(adds=tareas_creadas)
                print("Tareas de comisario creadas:", resp_tareas)
            else:
                print("No se crearon tareas de comisario.")
        except Exception as e:
            print("❌ Error al llamar edit_features(adds=...):")
            traceback.print_exc()
            return

        # Actualizar denuncias y comisarios (updates)
        try:
            if denuncias_actualizadas:
                resp_denuncias = layer_denuncias.edit_features(updates=denuncias_actualizadas)
                print("Denuncias actualizadas:", resp_denuncias)
            if comisarios_actualizados:
                resp_comisarios = tabla_comisarios.edit_features(updates=comisarios_actualizados)
                print("Comisarios actualizados:", resp_comisarios)
        except Exception as e:
            print("❌ Error al actualizar denuncias/comisarios:")
            traceback.print_exc()
            return

    except Exception as e_main:
        print("❌ Error general en la ejecución:")
        traceback.print_exc()

if __name__ == "__main__":
    ejecutar_asignacion_comisario()
