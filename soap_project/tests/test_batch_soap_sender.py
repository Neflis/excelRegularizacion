import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import patch, mock_open, MagicMock, call
import pathlib

# It's better to import the specific functions and constants you need
from soap_batch.batch_soap_sender import (
    generar_cuerpo_soap,
    main as batch_main, # Alias to avoid conflict with pytest main
    EXPECTED_COLUMNS
)

# Ensure the global EXPECTED_COLUMNS in the module is the one we test against
# This is generally not needed if the import is direct and the module doesn't modify it at runtime
# For safety, if batch_soap_sender.py somehow changed its own EXPECTED_COLUMNS based on some condition (it doesn't),
# we might want to reset it here. For now, direct import is fine.

@pytest.fixture
def sample_row_data_datetime(request):
    # Allows parametrizing the date for different test cases
    fecha_param = request.param if hasattr(request, "param") and request.param else datetime(2023, 10, 26, 10, 30, 0) # Default
    return pd.Series({
        "CDIAPTO": "MAD",
        "FECHA_EVENTO": fecha_param,
        "PNR_CODE": "ABC123",
        "ASIENTO": "10A",
        "TARJETA_FIDELIZACION": "F123456"
    })

@pytest.fixture
def sample_row_data_str_date(request):
    fecha_param = request.param if hasattr(request, "param") and request.param else "2023-10-26" # Default
    return pd.Series({
        "CDIAPTO": "BCN",
        "FECHA_EVENTO": fecha_param,
        "PNR_CODE": "XYZ789",
        "ASIENTO": "20B",
        "TARJETA_FIDELIZACION": "F789012"
    })

def test_generar_cuerpo_soap_con_datetime():
    datos_fila = pd.Series({
        "CDIAPTO": "MAD",
        "FECHA_EVENTO": datetime(2023, 10, 26, 11, 20, 30),
        "PNR_CODE": "ABC123",
        "ASIENTO": "10A",
        "TARJETA_FIDELIZACION": "F123456"
    })
    xml_generado = generar_cuerpo_soap(datos_fila)
    assert "<cdiApto>MAD</cdiApto>" in xml_generado
    assert "<fechaEvento>2023-10-26</fechaEvento>" in xml_generado # Check date format
    assert "<pnr>ABC123</pnr>" in xml_generado
    assert "<asiento>10A</asiento>" in xml_generado
    assert "<tarjetaFidelizacion>F123456</tarjetaFidelizacion>" in xml_generado
    assert 'soap12:Envelope' in xml_generado
    assert 'ns2:EventoPNR' in xml_generado

@pytest.mark.parametrize("sample_row_data_str_date", ["2024-01-15"], indirect=True)
def test_generar_cuerpo_soap_con_string_date_yyyy_mm_dd(sample_row_data_str_date):
    xml_generado = generar_cuerpo_soap(sample_row_data_str_date)
    assert "<cdiApto>BCN</cdiApto>" in xml_generado
    assert "<fechaEvento>2024-01-15</fechaEvento>" in xml_generado
    assert "<pnr>XYZ789</pnr>" in xml_generado

@pytest.mark.parametrize("sample_row_data_str_date", ["12/03/2025"], indirect=True) # DD/MM/YYYY format
def test_generar_cuerpo_soap_con_string_date_dd_mm_yyyy(sample_row_data_str_date):
    # pd.to_datetime is quite flexible. This test ensures our specific format conversion works.
    # The function uses pd.to_datetime(str_date).strftime('%Y-%m-%d')
    xml_generado = generar_cuerpo_soap(sample_row_data_str_date)
    assert "<fechaEvento>2025-03-12</fechaEvento>" in xml_generado # Should be converted

@pytest.mark.parametrize("sample_row_data_str_date", ["Mar 03, 2025"], indirect=True) # Another format
def test_generar_cuerpo_soap_con_string_date_mmm_dd_yyyy(sample_row_data_str_date):
    xml_generado = generar_cuerpo_soap(sample_row_data_str_date)
    assert "<fechaEvento>2025-03-03</fechaEvento>" in xml_generado

# --- Pruebas para la lógica de main (simulada) ---

@pytest.fixture
def mock_main_args(tmp_path):
    # tmp_path es una fixture de pytest que provee un directorio temporal único
    excel_dir = tmp_path / "excel_data"
    excel_dir.mkdir()
    return MagicMock(excel_dir=excel_dir, soap_endpoint="http://mock-endpoint.com")

@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap')
@patch('soap_batch.batch_soap_sender.log_soap_request')
@patch('pandas.read_excel')
def test_main_df_columnas_correctas(mock_read_excel, mock_log_soap, mock_enviar_soap, mock_main_args, capsys):
    # Datos de prueba para el DataFrame
    df_valid = pd.DataFrame([{
        "CDIAPTO": "MAD", "FECHA_EVENTO": "2023-01-01", "PNR_CODE": "PNR001",
        "ASIENTO": "1A", "TARJETA_FIDELIZACION": "TF001"
    }])
    mock_read_excel.return_value = df_valid
    
    # Simular que el archivo existe creando un dummy .xlsx en el directorio temporal
    dummy_file = mock_main_args.excel_dir / "valid_data.xlsx"
    dummy_file.touch()

    # Mock de la respuesta de la API SOAP
    mock_soap_response = MagicMock()
    mock_soap_response.status_code = 200
    mock_soap_response.text = "Success"
    mock_enviar_soap.return_value = mock_soap_response

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()

    captured = capsys.readouterr()
    assert "Procesando archivo: valid_data.xlsx" in captured.out
    mock_enviar_soap.assert_called_once()
    mock_log_soap.assert_any_call(pathlib.Path("soap_log.csv"), "valid_data.xlsx", 2, 200, "OK", "")
    assert "Filas enviadas exitosamente: 1" in captured.out


@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap')
@patch('soap_batch.batch_soap_sender.log_soap_request')
@patch('pandas.read_excel')
def test_main_df_columnas_faltantes(mock_read_excel, mock_log_soap, mock_enviar_soap, mock_main_args, capsys):
    df_missing_cols = pd.DataFrame([
        {"CDIAPTO": "BCN", "PNR_CODE": "XYZ789"} # Faltan FECHA_EVENTO, ASIENTO, TARJETA_FIDELIZACION
    ])
    mock_read_excel.return_value = df_missing_cols

    dummy_file = mock_main_args.excel_dir / "missing_cols.xlsx"
    dummy_file.touch()

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()
    
    captured = capsys.readouterr()
    assert "Procesando archivo: missing_cols.xlsx" in captured.out
    assert "WARNING: Archivo missing_cols.xlsx omitido. Cabecera no contiene todas las columnas esperadas." in captured.out
    mock_enviar_soap.assert_not_called()
    
    # Verificar que log_soap_request fue llamado con los detalles de omisión de cabecera
    expected_log_call_args = [
        pathlib.Path("soap_log.csv"),
        "missing_cols.xlsx",
        "N/A",
        "N/A",
        "OMITIDO_CABECERA" 
    ]
    # Hacemos la aserción más flexible para el último argumento (detalle del error)
    # ya que el orden de las columnas faltantes puede variar
    called_args_list = [c.args for c in mock_log_soap.call_args_list]
    found_log = False
    for called_args in called_args_list:
        if called_args[:4] == tuple(expected_log_call_args[:4]) and expected_log_call_args[4] in called_args[4]:
            # Check if the detail message contains the expected reason
            assert "FECHA_EVENTO" in called_args[5] 
            assert "ASIENTO" in called_args[5]
            assert "TARJETA_FIDELIZACION" in called_args[5]
            found_log = True
            break
    assert found_log, "Log de OMITIDO_CABECERA no encontrado o con detalles incorrectos"
    assert "Filas omitidas (archivo con cabecera incorrecta, o fila con datos nulos/faltantes): 1" in captured.out


@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap')
@patch('soap_batch.batch_soap_sender.log_soap_request')
@patch('pandas.read_excel')
def test_main_df_fila_con_datos_nulos(mock_read_excel, mock_log_soap, mock_enviar_soap, mock_main_args, capsys):
    df_nulos = pd.DataFrame([
        {"CDIAPTO": "VAL", "FECHA_EVENTO": "2023-11-10", "PNR_CODE": "PQR456", "ASIENTO": None, "TARJETA_FIDELIZACION": "F98765"},
        {"CDIAPTO": "LIS", "FECHA_EVENTO": "2023-11-11", "PNR_CODE": "LMN789", "ASIENTO": "12B", "TARJETA_FIDELIZACION": "F123098"}
    ])
    mock_read_excel.return_value = df_nulos
    
    dummy_file = mock_main_args.excel_dir / "nulos.xlsx"
    dummy_file.touch()

    mock_soap_response = MagicMock()
    mock_soap_response.status_code = 200
    mock_soap_response.text = "Success"
    mock_enviar_soap.return_value = mock_soap_response

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()

    captured = capsys.readouterr()
    assert "Procesando archivo: nulos.xlsx" in captured.out
    assert "WARNING: Fila 2 en nulos.xlsx omitida. Contiene valores nulos en columnas esperadas: ['ASIENTO']" in captured.out
    
    mock_enviar_soap.assert_called_once() # Solo se llama la fila válida
    
    # Verificar logs: uno por omisión, otro por éxito
    log_calls = mock_log_soap.call_args_list
    
    # Log para la fila omitida
    expected_omit_log = call(pathlib.Path("soap_log.csv"), "nulos.xlsx", 2, "N/A", "OMITIDO_NULOS", "Contiene valores nulos en columnas esperadas: ['ASIENTO']")
    # Log para la fila enviada
    expected_ok_log = call(pathlib.Path("soap_log.csv"), "nulos.xlsx", 3, 200, "OK", "")
    
    assert expected_omit_log in log_calls
    assert expected_ok_log in log_calls
    
    assert "Filas enviadas exitosamente: 1" in captured.out
    assert "Filas omitidas (archivo con cabecera incorrecta, o fila con datos nulos/faltantes): 1" in captured.out

@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap')
@patch('soap_batch.batch_soap_sender.log_soap_request')
@patch('pandas.read_excel')
def test_main_error_conexion_soap(mock_read_excel, mock_log_soap, mock_enviar_soap, mock_main_args, capsys):
    df_valid = pd.DataFrame([{
        "CDIAPTO": "SVQ", "FECHA_EVENTO": "2023-02-01", "PNR_CODE": "PNR002",
        "ASIENTO": "2B", "TARJETA_FIDELIZACION": "TF002"
    }])
    mock_read_excel.return_value = df_valid
    
    dummy_file = mock_main_args.excel_dir / "conexion_error.xlsx"
    dummy_file.touch()

    mock_enviar_soap.return_value = None # Simula error de conexión/timeout

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()

    captured = capsys.readouterr()
    assert "ERROR DE CONEXIÓN: Fila 2, PNR PNR002. Error de conexión o timeout" in captured.out
    mock_log_soap.assert_any_call(pathlib.Path("soap_log.csv"), "conexion_error.xlsx", 2, "N/A", "ERROR_CONEXION", "Error de conexión o timeout")
    assert "Filas con fallo en el envío (conexión, HTTP error, o error procesando fila): 1" in captured.out

@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap')
@patch('soap_batch.batch_soap_sender.log_soap_request')
@patch('pandas.read_excel')
def test_main_error_http_soap(mock_read_excel, mock_log_soap, mock_enviar_soap, mock_main_args, capsys):
    df_valid = pd.DataFrame([{
        "CDIAPTO": "OPO", "FECHA_EVENTO": "2023-03-01", "PNR_CODE": "PNR003",
        "ASIENTO": "3C", "TARJETA_FIDELIZACION": "TF003"
    }])
    mock_read_excel.return_value = df_valid

    dummy_file = mock_main_args.excel_dir / "http_error.xlsx"
    dummy_file.touch()

    mock_soap_response = MagicMock()
    mock_soap_response.status_code = 500
    mock_soap_response.text = "Internal Server Error"
    mock_enviar_soap.return_value = mock_soap_response

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()

    captured = capsys.readouterr()
    assert "ERROR HTTP: Fila 2, PNR PNR003, Status: 500, Msg: Internal Server Error" in captured.out
    mock_log_soap.assert_any_call(pathlib.Path("soap_log.csv"), "http_error.xlsx", 2, 500, "ERROR_HTTP", "Internal Server Error")
    assert "Filas con fallo en el envío (conexión, HTTP error, o error procesando fila): 1" in captured.out

# Prueba para verificar que el archivo de log se crea y tiene la cabecera correcta
@patch('soap_batch.batch_soap_sender.enviar_solicitud_soap') # Necesario para que main corra
@patch('pandas.read_excel') # Necesario para que main corra
def test_log_file_creation_and_header(mock_read_excel, mock_enviar_soap, mock_main_args, tmp_path):
    # Configurar mocks para que la ejecución de main sea mínima pero cree el log
    mock_read_excel.return_value = pd.DataFrame(columns=list(EXPECTED_COLUMNS)) # DF vacío pero con columnas
    
    # Simular un archivo excel para que el bucle de archivos se ejecute
    dummy_excel_for_log_test = mock_main_args.excel_dir / "log_header_test.xlsx"
    dummy_excel_for_log_test.touch()

    batch_main_args = ["--excel-dir", str(mock_main_args.excel_dir), "--soap-endpoint", mock_main_args.soap_endpoint]
    
    # El archivo de log se crea en el directorio actual donde se ejecuta el test
    # Necesitamos asegurar que es el path esperado (relativo a donde se ejecuta pytest)
    expected_log_path = pathlib.Path("soap_log.csv") 
    if expected_log_path.exists():
        expected_log_path.unlink() # Eliminar si existe de una ejecución anterior

    with patch('sys.argv', ['batch_soap_sender.py'] + batch_main_args):
        batch_main()

    assert expected_log_path.exists()
    with open(expected_log_path, 'r') as f:
        header = f.readline().strip()
        assert header == '"nombre_archivo","numero_linea","http_status","resultado","detalle_error"'
    
    # Limpiar el archivo de log creado
    if expected_log_path.exists():
        expected_log_path.unlink()

```
