from arcgis.gis import GIS
import pandas as pd
from arcgis.features import Feature
from datetime import timedelta, datetime
import html
import os

print("🟡 Script de supervisión iniciado...")

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

    # Listas de actualizaciones
    informes_actualizados = []
    tareas_creadas = []

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

        # Campos adicionales
        tipo_infraccion = row.get("infractor", "Sin especificar")
        direccion_infraccion = row.get("direccion", "Sin referencia")
        inspector = row.get("inspector_inspeccion", "No registrado")
        cedula_infractor = row.get("cedula_infractor", "No registrado")
        nombre_denunciado = row.get("nombre_denunciado", "No registrado")
        antecedentes = row.get("antecedentes", "---")
        desarrollo = row.get("desarrollo", "---")
        conclusiones = row.get("conclusiones", "---")

        descripcion_tarea = (
            f"Infracción reportada: {tipo_infraccion}\n"
            f"Referencia: {direccion_infraccion}\n"
            f"Inspector: {inspector}\n"
            f"Cedula Infractor: {cedula_infractor}\n"
            f"Nombre denunciado: {nombre_denunciado}\n\n"
            f"Antecedentes:\n{antecedentes}\n\n"
            f"Desarrollo:\n{desarrollo}\n\n"
            f"Conclusiones:\n{conclusiones}"
        )

        # Fecha de vencimiento
        fecha_actual_str = row.get("fecha_actual")
        due_date = None
        if fecha_actual_str:
            try:
                fecha_actual = pd.to_datetime(fecha_actual_str)
                due_date = fecha_actual + timedelta(days=3)
            except Exception as e:
                print(f"Error al convertir fecha_actual: {e}")

        
        # Corregir description antes de crear la tarea
        descripcion_tarea = descripcion_tarea.replace("<NA>", "No disponible")
        descripcion_tarea = html.escape(descripcion_tarea)
        
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
                "assigneddate": datetime.utcnow()
            },
            "geometry": geometry
        })
        tareas_creadas.append(tarea)

)

if result.features:
    print("✅ Registro encontrado en layer_denuncias:")
    atributos = result.features[0].attributes
    for k, v in atributos.items():
        print(f"   {k}: {v}")
else:
    print("❌ No se encontró ningún registro con ese GLOBALID en layer_denuncias")

     

        # Actualizar estado
        informes_actualizados.append(Feature.from_dict({
            "attributes": {
                "objectid": row["objectid"],
                "estado_tramite": "En supervisión"
            }
        }))

    # Tomar el GlobalID de la denuncia actual
    globalid_test = row["globalid"]

    print("🔎 Probando GlobalID:", globalid_test)

    # Hacer consulta al layer para comprobar que existe
    result = layer_denuncias.query(
    where=f"GLOBALID = '{globalid_test}'",
    out_fields="*",
    return_geometry=False

    # Guardar tareas
    if tareas_creadas:
        resp_tareas = layer_asignaciones.edit_features(adds=tareas_creadas)
        print("Tareas de supervisión creadas:", resp_tareas)

        # Asociar adjuntos
        for i, result in enumerate(resp_tareas.get("addResults", [])):
            if result.get("success"):
                oid_tarea = result.get("objectId")
                oid_informe = df_informes.iloc[i]["objectid"]
                adjuntos = layer_denuncias.attachments.get_list(oid=oid_informe)
                for adj in adjuntos:
                    try:
                        contenido = layer_denuncias.attachments.download(
                            oid=int(oid_informe),
                            attachment_id=adj["id"]
                        )
                        if isinstance(contenido, list) and contenido:
                            layer_asignaciones.attachments.add(oid_tarea, contenido[0])
                            print(f"Adjunto '{adj['name']}' copiado a la tarea.")
                    except Exception as e:
                        print(f"❌ Error al copiar adjunto '{adj['name']}': {e}")

    # Actualizar informes
    if informes_actualizados:
        resp_informes = layer_denuncias.edit_features(updates=informes_actualizados)
        print("Informes actualizados:", resp_informes)

if __name__ == "__main__":
    ejecutar_asignacion_supervision()

