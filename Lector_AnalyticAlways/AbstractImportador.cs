using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Lector_AnalyticAlways
{
    public abstract class AbstractImportador : IImportador
    {

        /// <summary>
        /// Método de importación principal. Podría haberse refactorizado ya que salvo un mensaje personalizado es común a todas las clases. No obstante 
        /// se ha mantenido en cada una por legibilidad de las mismas, en desarrollo de producción sería común a todas implementando únicamente los métodos de
        /// carga de datos especializados.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="cadConex"></param>
        public abstract void Importar(string path, string cadConex);
        protected abstract void _cargarDatos(SqlConnection con, string path);

        /// <summary>
        /// Procedimiento que realiza un borrado inicial de la BBDD.
        /// </summary>
        /// <param name="con">Conexión a BBDD</param>
        protected void _borrarTabla(SqlConnection con)
        {
            try
            {
                SqlCommand com = new SqlCommand("TRUNCATE TABLE STOCK", con);
                com.ExecuteNonQuery();
            }
            catch (Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error al borrar tabla en operación de importación." + exc.Message + " --> " + exc.StackTrace);
            }


        }

    
    }
}
