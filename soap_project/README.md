# Batch SOAP Sender from Excel

## Descripción Breve

Esta utilidad procesa archivos Excel (`.xlsx` y `.xls`) de un directorio especificado. Lee cada fila, verifica la presencia de columnas requeridas y que estas no contengan datos nulos. Por cada fila válida, genera un mensaje SOAP 1.2 y lo envía a un endpoint configurable. Los resultados de cada envío (éxito o error, junto con detalles relevantes como el código de estado HTTP) se registran en un archivo `soap_log.csv` y se muestran en consola. Al finalizar, se presenta un resumen del proceso.

## Arquitectura y Flujo Principal

1.  **Lectura de Archivos:**
    *   Se utiliza `pathlib` para buscar archivos `.xlsx` y `.xls` en el directorio proporcionado.
    *   La biblioteca `pandas` se emplea para leer los datos de las hojas de cálculo. Se usa el motor `openpyxl` para archivos `.xlsx` y `xlrd` para archivos `.xls`.

2.  **Validación de Datos:**
    *   Se comprueba que la cabecera de cada archivo Excel contenga las columnas esperadas: `CDIAPTO`, `FECHA_EVENTO`, `PNR_CODE`, `ASIENTO`, `TARJETA_FIDELIZACION`.
    *   Las filas que no contengan todas estas columnas o que tengan valores nulos en alguna de ellas son omitidas y se registra un aviso.

3.  **Generación SOAP:**
    *   Para cada fila válida, se construye un mensaje XML en formato SOAP 1.2. Actualmente, esto se realiza mediante f-strings.
    *   La columna `FECHA_EVENTO` se formatea a `YYYY-MM-DD` antes de incluirla en el cuerpo SOAP.

4.  **Envío SOAP:**
    *   La biblioteca `requests` se utiliza para enviar las solicitudes SOAP mediante el método POST al endpoint especificado por el usuario.
    *   Se establece un timeout para las solicitudes.

5.  **Logging:**
    *   Cada intento de envío (exitoso o fallido) se registra en un archivo `soap_log.csv` ubicado en el directorio desde donde se ejecuta el script.
    *   El log incluye: nombre del archivo Excel, número de línea original, código de estado HTTP (si la solicitud se completó), resultado ("OK" o "ERROR"), y un mensaje detallado en caso de error (ej., error de conexión, respuesta HTTP no exitosa, error al procesar la fila).
    *   También se muestran mensajes informativos y de error en la consola durante el procesamiento.

6.  **Resumen:**
    *   Al finalizar todas las operaciones, la utilidad muestra un resumen en consola que incluye:
        *   Total de archivos procesados.
        *   Total de filas leídas inicialmente.
        *   Total de filas que se intentaron procesar (válidas).
        *   Filas enviadas exitosamente.
        *   Filas con fallo en el envío (incluyendo errores de conexión, HTTP o de procesamiento de datos).
        *   Filas omitidas (debido a cabeceras incorrectas en el archivo o datos nulos/faltantes en la fila).

## Requisitos Previos

*   Python 3.10 o superior.
*   Las dependencias listadas en el archivo `requirements.txt`.

## Instalación

1.  Clona el repositorio (si aplica) o descarga los archivos del proyecto.
2.  Abre una terminal en el directorio raíz del proyecto (`soap_project`).
3.  Crea un entorno virtual (recomendado):
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # En Linux/macOS
    # .venv\Scripts\activate    # En Windows
    ```
4.  Instala las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso (Ejemplo de Línea de Comandos)

Ejecuta el script desde el directorio raíz del proyecto (`soap_project`) de la siguiente manera:

```bash
python -m soap_batch.batch_soap_sender \
       --excel-dir /ruta/absoluta/a/tu/directorio_excel \
       --soap-endpoint https://tu.api.ejemplo.com/soapservice
```

*   **`--excel-dir`**: Especifica la ruta al directorio que contiene los archivos Excel a procesar. Utiliza rutas absolutas o relativas al directorio actual.
*   **`--soap-endpoint`**: La URL completa del servicio SOAP al que se enviarán las solicitudes.

**Ejemplo:**

```bash
python -m soap_batch.batch_soap_sender \
       --excel-dir ./datos_excel_entrada \
       --soap-endpoint https://mi.servidor.com/api/evento
```

Asegúrate de reemplazar `/ruta/absoluta/a/tu/directorio_excel` y la URL del endpoint con los valores correctos para tu caso. El archivo `soap_log.csv` se generará en el directorio donde ejecutes el comando.

## Estructura del Proyecto

```
soap_project/
├── soap_batch/
│   ├── __init__.py
│   └── batch_soap_sender.py
├── tests/
│   ├── __init__.py
│   └── test_batch_soap_sender.py
├── requirements.txt
└── README.md
```

## Pruebas

Las pruebas unitarias están implementadas con `pytest`. Para ejecutarlas:

1.  Asegúrate de tener `pytest` instalado (está en `requirements.txt`).
2.  Desde el directorio raíz del proyecto (`soap_project`), ejecuta:

    ```bash
    pytest
    ```
Esto descubrirá y ejecutará automáticamente las pruebas definidas en el directorio `tests/`.
