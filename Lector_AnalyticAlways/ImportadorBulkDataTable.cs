using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Lector_AnalyticAlways
{
    public class ImportadorBulkDataTable : AbstractImportador
    {

        public override void Importar(string path,string cadConex)
        {
            DateTime inicio = DateTime.Now;
            
            string cadenaConexion = cadConex;
            
            SqlConnection con = new SqlConnection(cadenaConexion);
            try
            {
                con.Open();

                _borrarTabla(con);

                _cargarDatos(con, path);

                Console.WriteLine("Tiempo de ejecución " + DateTime.Now.Subtract(inicio).Milliseconds.ToString());
            }
            catch(LectorAnalyticAlwaysException lAAExc)
            {
                throw lAAExc;
            }
            catch(Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error no capturado en ImportadorBulkDataTable." + exc.Message + " --> " + exc.StackTrace);
            }
            finally
            {
                con.Close();
                con.Dispose();
            }
           
        }

        protected override void _cargarDatos(SqlConnection con, string path)
        {

            try
            {
                DataTable csvFileData = _getDataTabletFromCSVFile(path);

                SqlBulkCopy bulkCopyManager = new SqlBulkCopy(con);
                bulkCopyManager.DestinationTableName = "STOCK";
                foreach (var column in csvFileData.Columns)
                {
                    bulkCopyManager.ColumnMappings.Add(column.ToString(), column.ToString());
                }
                bulkCopyManager.WriteToServer(csvFileData);

                bulkCopyManager.Close();
            }
            catch(LectorAnalyticAlwaysException lAAExc)
            {
                throw lAAExc;
            }
            catch(Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error al cargar datos." + exc.Message + " --> " + exc.StackTrace);
            }
        }

        private DataTable _getDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { ";" });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column.ToUpper());
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error al cargar DataTable desde fichero CSV." + exc.Message + " --> " + exc.StackTrace);
            }
            return csvData;
        }
    }
}

