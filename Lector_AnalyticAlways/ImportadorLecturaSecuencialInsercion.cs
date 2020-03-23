using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace Lector_AnalyticAlways
{

    public class ImportadorLecturaSecuencialInsercion : AbstractImportador
    {
        public override void Importar(string path, string cadConex)
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
                throw new LectorAnalyticAlwaysException("Error no capturado en ImportadorLecturaSecuencialInsercion." + exc.Message + " --> " + exc.StackTrace);
            }
            finally
            {
                con.Close();
                con.Dispose();
            }
        }

        protected override void _cargarDatos(SqlConnection con, string path)
        {
            StreamReader reader = new StreamReader(new FileStream(path, FileMode.Open, FileAccess.Read));
            string cabecera = reader.ReadLine();
            string fila = string.Empty;

            while (!reader.EndOfStream)
            {
                _tratarFila(reader.ReadLine(), con);

            }
           
        }

        private void _tratarFila(string fila, SqlConnection con)
        {
            try
            {
                string[] datos = fila.Split(";");

                SqlCommand com = new SqlCommand("INSERT INTO STOCK (POINTOFSALE, PRODUCT, DATE, STOCK) VALUES ('" + datos[0] + "','" + datos[1] + "','" + datos[2] + "'," + datos[3] + ")", con);
                com.ExecuteNonQuery();
            }
            catch(Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error al tratar fila " + fila + " en operación de importación." + exc.Message + " --> " + exc.StackTrace);
            }
        }
    }
}
