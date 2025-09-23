from arcgis.gis import GIS
import pandas as pd
from arcgis.features import Feature
from datetime import timedelta, datetime
import os

print("🟡 Script asignar_comisario iniciado...")

def ejecutar_asignacion_comisario():
    usuario = os.getenv("AGOL_USERNAME")
    clave = os.getenv("AGOL_PASSWORD")
    if not usuario or not clave:
        print("❌ No se encontraron credenciales en las variables de entorno.")
        return
    try:
        gis = GIS("https://www.arcgis.com", usuario, clave)
        print(f"🟢 Sesión iniciada como: {gis.users.me.username}")
    except Exception as e:
        print(f"❌ Error al iniciar sesión en ArcGIS Online: {e}")
        return

    # Tabla comisarios
    item_tabla = gis.content.get("aa7cb6814d7d44beaa2557533103e7aa")
    # Denuncias
    item_denuncia = gis.content.get("60c69b82ab074b65a8a239fcd2067ce4")
    # Workforce
    item_workforce = gis.content.get("bf86d367917747cf82fb57a9128eed0e")

    tabla_comisarios = item_tabla.tables[0]
    layer_denuncias = item_denuncia.layers[0]
    layer_asignaciones = item_workforce.layers[0]
    layer_workers = item_workforce.layers[1]

    # Consultas
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

    denuncias_actualizadas = []
    comisarios_actualizados = []
    tareas_creadas = []

    # GUID del tipo de asignación "Comisario"
    assignmenttype_guid = "33aec22e-5094-4cce-9493-a3444d8fba8c"

    for _, row in df_denuncias.iterrows():
        if df_comisarios.empty:
            print("❌ No hay comisarios disponibles en la tabla")
            continue

        # Seleccionar comisario con menos trámites (sin filtrar por dirección/área)
        comisario_asignado = df_comisarios.sort_values("num_tramites").iloc[0]

        # Generar número de formulario
        siglas_area = row.get("siglas_area", "XX")
        nombre_comisario = comisario_asignado["nombre"]
        siglas_comisario = comisario_asignado["siglas"]
        anio_actual = datetime.utcnow().year
        ultimo_numero = comisario_asignado.get("ultimo_numero", 0) + 1

        numero_formulario = f"DGSH-CO-{siglas_comisario}-{siglas_area}-{anio_actual}-{ultimo_numero}"

        # Actualizar denuncia
        feature_denuncia = Feature.from_dict({
            "attributes": {
                "objectid": row["objectid"],
                "comisario_asignado": nombre_comisario,
                "username": comisario_asignado["usernamearc"],
                "estado_tramite": "Asignado a comisario",
                "id_denuncia_comparar_comisario": str(row["globalid"])
            }
        })
        denuncias_actualizadas.append(feature_denuncia)

        # Actualizar comisario
        feature_comisario = Feature.from_dict({
            "attributes": {
                "objectid": comisario_asignado["ObjectID"],
                "num_tramites": comisario_asignado["num_tramites"] + 1,
                "ultimo_numero": ultimo_numero
            }
        })
        comisarios_actualizados.append(feature_comisario)

        # Buscar worker en Workforce
        worker_index = df_workers[df_workers["userid"] == comisario_asignado["usernamearc"]].index
        if worker_index.empty:
            print(f"No se encontró al trabajador {comisario_asignado['usernamearc']} en Workforce")
            continue

        worker_feature = features_workers.features[worker_index[0]]
        worker_globalid = worker_feature.attributes["GlobalID"]

        # Geometría
        shape_dict = row.get("SHAPE") or row.get("geometry")
        geometry = None
        if shape_dict:
            geometry = {
                "x": shape_dict.get("x"),
                "y": shape_dict.get("y"),
                "spatialReference": {"wkid": 4326}
            }

        # Descripción de la tarea
        descripcion_tarea = (
            f"Informe de supervisión finalizada\n"
            f"Infracción: {row.get('tipo_infraccion','')}\n"
            f"Infractor: {row.get('denunciado','')}\n"
            f"Proceso administrativo: {row.get('proceso_administrativo','')}\n"
        )

        # Fecha límite (3 días desde hoy)
        due_date = datetime.utcnow() + timedelta(days=3)

        tarea = Feature.from_dict({
            "attributes": {
                "description": descripcion_tarea,
                "status": 1,
                "priority": 0,
                "assignmenttype": assignmenttype_guid,
                "location": row.get("area_responsable", ""),
                "workorderid": str(row["globalid"]),
                "codigoformulario": numero_formulario,
                "nombrecomisario": nombre_comisario,
                "workerid": worker_globalid,
                "duedate": due_date,
                "assigneddate": datetime.utcnow()
            },
            "geometry": geometry
        })
        tareas_creadas.append(tarea)

    # Guardar tareas
    if tareas_creadas:
        resp_tareas = layer_asignaciones.edit_features(adds=tareas_creadas)
        print("Tareas de comisario creadas:", resp_tareas)
    else:
        print("No se crearon tareas de comisario.")

    # Actualizar denuncias y comisarios
    if denuncias_actualizadas:
        resp_denuncias = layer_denuncias.edit_features(updates=denuncias_actualizadas)
        print("Denuncias actualizadas:", resp_denuncias)

    if comisarios_actualizados:
        resp_comisarios = tabla_comisarios.edit_features(updates=comisarios_actualizados)
        print("Comisarios actualizados:", resp_comisarios)

if __name__ == "__main__":
    ejecutar_asignacion_comisario()
