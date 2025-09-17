from arcgis.gis import GIS
import pandas as pd
from arcgis.features import Feature
from datetime import timedelta, datetime
import os

print("🟡 Script de supervisión iniciado...")

def to_epoch_millis(dt):
    """Convierte datetime a epoch en milisegundos"""
    return int(dt.timestamp() * 1000) if dt else None

def ejecutar_asignacion_supervision():
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

    # Items
    item_denuncia = gis.content.get("60c69b82ab074b65a8a239fcd2067ce4")  # registro_infracciones-gadmr
    item_workforce = gis.content.get("bf86d367917747cf82fb57a9128eed0e")  # Workforce

    # Capas
    layer_denuncias = item_denuncia.layers[0]
    layer_asignaciones = item_workforce.layers[0]
    layer_workers = item_workforce.layers[1]

    # Consultar informes con estado "Informe enviado"
    features_denuncias = layer_denuncias.query(
        where="estado_tramite = 'Informe enviado'", 
        out_fields="*", 
        return_geometry=True
    )
    df_informes = features_denuncias.sdf

    print(f"Total de informes para supervisión encontrados: {len(df_informes)}")

    if df_informes.empty:
        return

    # GUID del tipo de asignación "Supervisión"
    assignmenttype_guid = "52de28ac-8476-42ca-8e16-d8b7872ad3c5"

    # Usuario fijo del supervisor
    supervisor_user = "coellop_gadmriobamba"
    features_workers = layer_workers.query(
        where=f"userid='{supervisor_user}'", 
        out_fields="*", 
        return_geometry=False
    )
    if not features_workers.features:
        print(f"❌ No se encontró el supervisor {supervisor_user} en Workforce.")
        return
    worker_globalid = features_workers.features[0].attributes["GlobalID"]

    tareas_creadas = []
    informes_actualizados = []

    for _, row in df_informes.iterrows():
        # Geometría
        shape_dict = row.get("SHAPE") or row.get("geometry")
        geometry = None
        if shape_dict:
            geometry = {
                "x": shape_dict.get("x"),
                "y": shape_dict.get("y"),
                "spatialReference": {"wkid": 4326}
            }

        # Datos del informe
        descripcion_tarea = (
            f"Infracción: {row.get('tipo_infraccion','-')}\n"
            f"Referencia: {row.get('direccion_infraccion','-')}\n"
            f"Denunciado: {row.get('denunciado','-')}\n"
            f"CI Infractor: {row.get('cedula_infractor','-')}\n"
            f"Nombre denunciado: {row.get('nombre_denunciado','-')}\n"
            f"Antecedentes: {row.get('antecedentes','---')}\n"
            f"Desarrollo: {row.get('desarrollo','---')}\n"
            f"Conclusiones: {row.get('conclusiones','---')}"
        )

        # Fechas en epoch
        fecha_actual_str = row.get("fecha_actual")
        due_date = None
        if fecha_actual_str:
            try:
                fecha_actual = pd.to_datetime(fecha_actual_str)
                due_date = to_epoch_millis(fecha_actual + timedelta(days=3))
            except Exception as e:
                print(f"⚠️ Error al convertir fecha_actual: {e}")
        assigneddate = to_epoch_millis(datetime.utcnow())

        # Crear tarea
        tarea = Feature.from_dict({
            "attributes": {
                "description": descripcion_tarea,
                "status": 1,
                "priority": 0,
                "assignmenttype": assignmenttype_guid,
                "location": row.get("direccion", "Sin área"),
                "workorderid": str(row["globalid"]),
                "workerid": worker_globalid,
                "duedate": due_date,
                "assigneddate": assigneddate
            },
            "geometry": geometry
        })
        tareas_creadas.append(tarea)

        # Actualizar estado
        informes_actualizados.append(
            Feature.from_dict({
                "attributes": {
                    "objectid": row["objectid"],
                    "estado_tramite": "En supervisión"
                }
            })
        )

    # Guardar solo la primera tarea como prueba
    if tareas_creadas:
        print(">>> Debug: cantidad de tareas creadas =", len(tareas_creadas))
        for i, t in enumerate(tareas_creadas):
            try:
                print(f"--- Tarea #{i+1} ---")
                print(t.as_dict)  # 👈 ver contenido exacto
            except Exception as e:
                print(f"❌ No se pudo convertir tarea #{i+1}:", e)

        print(">>> Intentando guardar solo la primera tarea...")
        try:
            resp_tareas = layer_asignaciones.edit_features(adds=[tareas_creadas[0]])
            print("Respuesta Workforce:", resp_tareas)
        except Exception as e:
            print("❌ Error al guardar tarea:", e)

    # Actualizar informes
    if informes_actualizados:
        try:
            resp_informes = layer_denuncias.edit_features(updates=informes_actualizados)
            print("Informes actualizados:", resp_informes)
        except Exception as e:
            print("❌ Error al actualizar informes:", e)

if __name__ == "__main__":
    ejecutar_asignacion_supervision()
