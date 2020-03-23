using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace Lector_AnalyticAlways
{
    /// <summary>
    /// Clase de importación que emplea un Procedimiento Almacenado en BBDD para hacer un volcado directo del fichero.
    /// </summary>
    public class ImportadorPorPA : AbstractImportador
    {
        public override void Importar(string path, string cadConex)
        {
            string cadenaConexion = cadConex;

            
            SqlConnection con = new SqlConnection(cadenaConexion);

            try
            {
                con.Open();

                _borrarTabla(con);

                _cargarDatos(con, path);

            }
            catch(LectorAnalyticAlwaysException lAAExc)
            {
                throw lAAExc;
            }
            catch(Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error no capturado en ImportadorPorPA." + exc.Message + " --> " + exc.StackTrace);
            }
            finally
            {
                con.Close();
                con.Dispose();

            }

        }

        /// <summary>
        /// Método que realiza la carga de datos llamando al procedimiento almacenado.
        /// </summary>
        /// <param name="con">Conexión a BBDD</param>
        /// <param name="path">Path con el fichero CSV origen.</param>
        protected override void _cargarDatos(SqlConnection con, string path)
        {
            try
            {
                SqlCommand command = new SqlCommand("dbo.BULK_LOAD",con);
                command.CommandTimeout = 0;
                command.CommandType = CommandType.StoredProcedure;
                
                SqlParameter param = new SqlParameter("@PATH","'" +path +"'");
                param.SqlDbType = SqlDbType.NVarChar;
                command.Parameters.Add(param);

                SqlParameter param2 = new SqlParameter("@BATCHSIZE", 1000000);
                param2.SqlDbType = SqlDbType.Int;
                command.Parameters.Add(param2);

                command.ExecuteNonQuery();
            }
            catch(Exception exc) {

                throw new LectorAnalyticAlwaysException("Error al cargar datos en operación de importación por PA." + exc.Message + "\\r\\n" + exc.StackTrace);
            }
                
            
        }

     
    }
}
