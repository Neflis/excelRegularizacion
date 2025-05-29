import argparse
import pathlib
import pandas as pd
import requests
from lxml import etree
import csv # Importar el módulo csv

# Definición de las columnas esperadas
EXPECTED_COLUMNS = {"CDIAPTO", "FECHA_EVENTO", "PNR_CODE", "ASIENTO", "TARJETA_FIDELIZACION"}

def generar_cuerpo_soap(fila_datos):
    # Asegurarse de que FECHA_EVENTO esté en formato YYYY-MM-DD
    fecha_evento_str = fila_datos["FECHA_EVENTO"]
    if isinstance(fecha_evento_str, pd.Timestamp):
        fecha_evento_str = fecha_evento_str.strftime('%Y-%m-%d')
    elif not isinstance(fecha_evento_str, str): # Si no es Timestamp ni string, intentar convertir
        try:
            fecha_evento_str = pd.to_datetime(fecha_evento_str).strftime('%Y-%m-%d')
        except Exception:
            # Si falla la conversión, se usará tal cual, pero podría ser un problema.
            # Considerar loguear esta situación o lanzar un error específico.
            pass


    return f'''<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <ns2:EventoPNR xmlns:ns2="http://ejemplo.com/eventoPNR/v1">
      <cdiApto>{fila_datos["CDIAPTO"]}</cdiApto>
      <fechaEvento>{fecha_evento_str}</fechaEvento>
      <pnr>{fila_datos["PNR_CODE"]}</pnr>
      <asiento>{fila_datos["ASIENTO"]}</asiento>
      <tarjetaFidelizacion>{fila_datos["TARJETA_FIDELIZACION"]}</tarjetaFidelizacion>
    </ns2:EventoPNR>
  </soap12:Body>
</soap12:Envelope>'''

def enviar_solicitud_soap(endpoint_url, soap_body_str):
    headers = {'Content-Type': 'application/soap+xml; charset=utf-8'}
    try:
        response = requests.post(endpoint_url, data=soap_body_str.encode('utf-8'), headers=headers, timeout=20) # Timeout de 20s
        return response
    except requests.exceptions.RequestException as e:
        # No imprimir aquí para no duplicar logs si el llamador ya lo hace.
        # print(f"Excepción durante la solicitud SOAP: {e}") 
        return None

def log_soap_request(log_file_path, nombre_archivo, numero_linea, http_status, resultado, detalle_error):
    # Usar csv.writer para manejar correctamente comas y comillas en los campos.
    with open(log_file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow([nombre_archivo, numero_linea, http_status, resultado, detalle_error])

def main():
    parser = argparse.ArgumentParser(description="Envía solicitudes SOAP basadas en datos de archivos Excel.")
    parser.add_argument(
        "--excel-dir",
        type=pathlib.Path,
        required=True,
        help="Directorio que contiene los archivos Excel a procesar."
    )
    parser.add_argument(
        "--soap-endpoint",
        type=str,
        required=True,
        help="URL del endpoint SOAP para enviar las solicitudes."
    )
    args = parser.parse_args()

    print(f"Directorio Excel: {args.excel_dir}")
    print(f"Endpoint SOAP: {args.soap_endpoint}")

    if not args.excel_dir.is_dir():
        print(f"Error: El directorio especificado no existe o no es un directorio: {args.excel_dir}")
        return

    total_filas_leidas = 0
    archivos_procesados = 0
    filas_procesadas_total = 0
    filas_enviadas_exitosamente = 0
    filas_con_fallo_envio = 0
    filas_omitidas_por_columnas_o_datos = 0 # Unificado para todos los tipos de omisiones previas al envío
    
    log_file_path = pathlib.Path("soap_log.csv")
    # Escribir la cabecera del CSV usando log_soap_request para asegurar consistencia de formato
    # Esto es un poco un truco, ya que log_soap_request espera datos de fila.
    # Una alternativa es abrir el archivo y escribir la cabecera directamente.
    with open(log_file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["nombre_archivo", "numero_linea", "http_status", "resultado", "detalle_error"])


    for file_extension in ['*.xlsx', '*.xls']:
        for file_path in args.excel_dir.glob(file_extension):
            engine = 'openpyxl' if file_extension == '*.xlsx' else 'xlrd'
            try:
                df = pd.read_excel(file_path, sheet_name=0, engine=engine)
                print(f"Procesando archivo: {file_path.name}")
                archivos_procesados += 1
                total_filas_leidas += len(df)

                columnas_actuales = set(df.columns)
                if not EXPECTED_COLUMNS.issubset(columnas_actuales):
                    columnas_faltantes = EXPECTED_COLUMNS - columnas_actuales
                    msg_error = f"Cabecera no contiene todas las columnas esperadas. Faltantes: {columnas_faltantes}"
                    print(f"WARNING: Archivo {file_path.name} omitido. {msg_error}")
                    # Loguear una entrada para todo el archivo omitido
                    log_soap_request(log_file_path, file_path.name, "N/A", "N/A", "OMITIDO_CABECERA", msg_error)
                    filas_omitidas_por_columnas_o_datos += len(df)
                    continue

                for index, row_original in df.iterrows():
                    filas_procesadas_total += 1
                    linea_excel = index + 2 # Para reportar al usuario (1-based + cabecera)
                    
                    # Validar datos de la fila
                    datos_fila_map = row_original[list(EXPECTED_COLUMNS)].copy() # Usar .copy() para evitar SettingWithCopyWarning
                    if datos_fila_map.isnull().any():
                        cols_nulas = datos_fila_map[datos_fila_map.isnull()].index.tolist()
                        msg_error = f"Contiene valores nulos en columnas esperadas: {cols_nulas}"
                        print(f"WARNING: Fila {linea_excel} en {file_path.name} omitida. {msg_error}")
                        log_soap_request(log_file_path, file_path.name, linea_excel, "N/A", "OMITIDO_NULOS", msg_error)
                        filas_omitidas_por_columnas_o_datos +=1
                        continue
                    
                    try:
                        # La conversión de fecha ahora está dentro de generar_cuerpo_soap
                        soap_body_str = generar_cuerpo_soap(datos_fila_map)
                        print(f"  Fila {linea_excel}: Enviando SOAP para PNR {datos_fila_map['PNR_CODE']}")
                        
                        response = enviar_solicitud_soap(args.soap_endpoint, soap_body_str)

                        if response is None:
                            error_detalle = "Error de conexión o timeout"
                            print(f"    ERROR DE CONEXIÓN: Fila {linea_excel}, PNR {datos_fila_map['PNR_CODE']}. {error_detalle}")
                            log_soap_request(log_file_path, file_path.name, linea_excel, "N/A", "ERROR_CONEXION", error_detalle)
                            filas_con_fallo_envio += 1
                        else:
                            if 200 <= response.status_code < 300:
                                print(f"    SUCCESS: Fila {linea_excel}, PNR {datos_fila_map['PNR_CODE']}, Status: {response.status_code}")
                                log_soap_request(log_file_path, file_path.name, linea_excel, response.status_code, "OK", "")
                                filas_enviadas_exitosamente += 1
                            else:
                                error_text = response.text.strip() if response.text else "Respuesta vacía"
                                print(f"    ERROR HTTP: Fila {linea_excel}, PNR {datos_fila_map['PNR_CODE']}, Status: {response.status_code}, Msg: {error_text[:100]}")
                                log_soap_request(log_file_path, file_path.name, linea_excel, response.status_code, "ERROR_HTTP", error_text)
                                filas_con_fallo_envio += 1
                    
                    except KeyError as e:
                        msg_error = f"Falta la columna esperada: {e}"
                        print(f"WARNING: Fila {linea_excel} en {file_path.name} omitida. {msg_error}")
                        log_soap_request(log_file_path, file_path.name, linea_excel, "N/A", "ERROR_DATOS_FILA", msg_error)
                        filas_omitidas_por_columnas_o_datos += 1
                        continue # Asegurarse de que continúa al siguiente ciclo de fila
                    except Exception as e:
                        msg_error = f"Error inesperado procesando fila: {str(e)}"
                        print(f"ERROR: Fila {linea_excel} en {file_path.name}. {msg_error}")
                        log_soap_request(log_file_path, file_path.name, linea_excel, "N/A", "ERROR_PROCESANDO_FILA", msg_error)
                        filas_con_fallo_envio += 1 # Contar como fallo de envío si no se pudo ni intentar enviar

            except Exception as e:
                msg_error = f"Error crítico leyendo o procesando el archivo: {str(e)}"
                print(f"ERROR: {file_path.name}. {msg_error}")
                log_soap_request(log_file_path, file_path.name, "N/A", "N/A", "ERROR_LECTURA_PROCESO_ARCHIVO", msg_error)
                # Si el archivo no se puede leer, no hay filas que contar como omitidas individualmente,
                # pero se podría añadir a un contador de archivos fallidos si fuera necesario.


    print(f"\n--- Resumen del Procesamiento ---")
    print(f"Total de archivos procesados (o intentados): {archivos_procesados}")
    print(f"Total de filas leídas de los archivos: {total_filas_leidas}")
    print(f"Total de filas procesadas (intentos de envío + omitidas individualmente): {filas_procesadas_total}")
    print(f"Filas enviadas exitosamente: {filas_enviadas_exitosamente}")
    print(f"Filas con fallo en el envío (conexión, HTTP error, o error procesando fila): {filas_con_fallo_envio}")
    print(f"Filas omitidas (archivo con cabecera incorrecta, o fila con datos nulos/faltantes): {filas_omitidas_por_columnas_o_datos}")
    print(f"Logs guardados en: {log_file_path.resolve()}")

if __name__ == "__main__":
    main()
