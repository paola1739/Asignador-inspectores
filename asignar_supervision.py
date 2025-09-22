from arcgis.gis import GIS
import pandas as pd
from arcgis.features import FeatureLayer, Feature
from datetime import timedelta, datetime
import os

print("🟡 Script iniciado...")  # <-- Rastreo inicial

def ejecutar_asignacion():
    print("🟡 Ejecutando función asignar supervisor")
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
    item_tabla = gis.content.get("a255f5953df24eb08917602c1d89885e")  # inspectores
    item_denuncia = gis.content.get("60c69b82ab074b65a8a239fcd2067ce4")  # denuncias
    item_workforce = gis.content.get("bf86d367917747cf82fb57a9128eed0e")  # workforce

    # Capas y tablas
    tabla_inspectores = item_tabla.tables[0]
    layer_denuncias = item_denuncia.layers[0]
    layer_asignaciones = item_workforce.layers[0]
    layer_workers = item_workforce.layers[1]

    # Consultas
    features_inspectores = tabla_inspectores.query(where="1=1", out_fields="*", return_geometry=False)
    features_denuncias = layer_denuncias.query(where="1=1", out_fields="*", return_geometry=True)
    features_workers = layer_workers.query(where="1=1", out_fields="*", return_geometry=False)

    df_inspectores = features_inspectores.sdf
    df_denuncias = features_denuncias.sdf
    df_workers = features_workers.sdf

    # Filtrar denuncias con estado "Recibido"
    df_nuevas = df_denuncias[df_denuncias["estado_tramite"] == "Informe enviado"].copy()
    print(f"Total de denuncias 'Recibido' encontradas: {len(df_nuevas)}")

    # Listas de actualizaciones
    denuncias_actualizadas = []
    inspectores_actualizados = []
    tareas_creadas = []

    # GUID del tipo de asignación "Inspeccion"
    assignmenttype_guid = "22309f2f-e893-4443-97eb-1b6944a27d00"

    for _, row in df_nuevas.iterrows():
        direccion = row["direccion_responsable"]
        area = row["area_responsable"]

        disponibles = df_inspectores[
            (df_inspectores["direccion"] == direccion) &
            (df_inspectores["area"] == area)
        ]

        if disponibles.empty:
            print(f"No hay inspectores activos para dirección: {direccion}, área: {area}")
            continue

        # Seleccionar inspector con menos trámites asignados
        inspector_asignado = disponibles.sort_values("num_tramites").iloc[0]

        # Generar número de formulario
        siglas_area = row["siglas_area"]  # viene de la denuncia
        nombre_inspector = inspector_asignado["nombre"]
        siglas_inspector = inspector_asignado["siglas"]
        anio_actual = datetime.utcnow().year
        ultimo_numero = inspector_asignado.get("ultimo_numero", 0) + 1

        numero_formulario = f"DGSH-IC-{siglas_inspector}-{siglas_area}-{anio_actual}-{ultimo_numero}"

        # ✅ Bloque de comprobación del GLOBALID
        globalid_test = row["globalid"]
        print("🔎 Probando GlobalID:", globalid_test)

        result = layer_denuncias.query(
            where=f"GLOBALID = '{globalid_test}'",
            out_fields="*",
            return_geometry=False
        )

        if result.features:
            print("✅ Registro encontrado en layer_denuncias:")
            atributos = result.features[0].attributes
            for k, v in atributos.items():
                print(f"   {k}: {v}")
        else:
            print("❌ No se encontró ningún registro con ese GLOBALID en layer_denuncias")

        # Actualizar denuncia
        feature_denuncia = Feature.from_dict({
            "attributes": {
                "objectid": row["objectid"],
                "inspector_asignado": inspector_asignado["nombre"],
                "username": inspector_asignado["usernamearc"],
                "estado_tramite": "En proceso",
                "id_denuncia_c": str(row["globalid"])
            }
        })
        denuncias_actualizadas.append(feature_denuncia)

        # Actualizar inspector
        feature_inspector = Feature.from_dict({
            "attributes": {
                "objectid": inspector_asignado["ObjectID"],
                "num_tramites": inspector_asignado["num_tramites"] + 1,
                "ultimo_numero": ultimo_numero
            }
        })
        inspectores_actualizados.append(feature_inspector)

        # Obtener GlobalID del trabajador
        worker_index = df_workers[df_workers["userid"] == inspector_asignado["usernamearc"]].index
        if worker_index.empty:
            print(f"No se encontró al trabajador {inspector_asignado['usernamearc']} en Workforce")
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

        # Campos adicionales para la descripción
        tipo_infraccion = row.get("tipo_infraccion", "Sin especificar")
        direccion_infraccion = row.get("direccion_infraccion", "Sin referencia")
        denunciado = row.get("denunciado", "No registrado")
        descripcion_infraccion = row.get("comentario_denuncia", "Sin detalle")
        contacto = row.get("contacto_denunciante_no", "No disponible")

        # Descripción formateada
        descripcion_tarea = (
            f"Infracción reportada: {tipo_infraccion}\n"
            f"Referencia: {direccion_infraccion}\n"
            f"Denunciado: {denunciado}\n"
            f"Información adicional: {descripcion_infraccion}\n"
            f"Contacto del denunciante: {contacto}"
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

        # Crear tarea
        tarea = Feature.from_dict({
            "attributes": {
                "description": descripcion_tarea,
                "status": 1,
                "priority": 0,
                "assignmenttype": assignmenttype_guid,
                "location": row["area_responsable"],
                "workorderid": str(row["globalid"]),
                "codigoformulario": numero_formulario,
                "nombreinspector": nombre_inspector,
                "workerid": worker_globalid,
                "duedate": due_date,
                "assigneddate": datetime.utcnow()
            },
            "geometry": geometry
        })
        tareas_creadas.append(tarea)

    # Guardar tareas y obtener sus IDs
    if tareas_creadas:
        respuesta_tareas = layer_asignaciones.edit_features(adds=tareas_creadas)
        print("Tareas creadas en Workforce:")
        print(respuesta_tareas)

        # Asociar adjuntos
        for i, result in enumerate(respuesta_tareas.get("addResults", [])):
            if result.get("success"):
                oid_tarea = result.get("objectId")
                oid_denuncia = df_nuevas.iloc[i]["objectid"]
                adjuntos = layer_denuncias.attachments.get_list(oid=oid_denuncia)

                for adj in adjuntos:
                    try:
                        contenido = layer_denuncias.attachments.download(
                            oid=int(oid_denuncia),
                            attachment_id=adj["id"]
                        )
                        if isinstance(contenido, list) and contenido:
                            resultado = layer_asignaciones.attachments.add(oid_tarea, contenido[0])
                        else:
                            print(f"⚠️ No se pudo descargar el adjunto '{adj['name']}' correctamente.")
                    except Exception as e:
                        print(f"❌ Excepción al copiar adjunto '{adj['name']}': {e}")
    else:
        print("No hay tareas para crear.")

    # Actualizar denuncias e inspectores
    if denuncias_actualizadas:
        respuesta_denuncias = layer_denuncias.edit_features(updates=denuncias_actualizadas)
        print("Actualización de denuncias:")
        print(respuesta_denuncias)
    else:
        print("No hay denuncias para actualizar.")

    if inspectores_actualizados:
        respuesta_inspectores = tabla_inspectores.edit_features(updates=inspectores_actualizados)
        print("Actualización de inspectores:")
        print(respuesta_inspectores)
    else:
        print("No hay inspectores para actualizar.")

if __name__ == "__main__":
    ejecutar_asignacion()
