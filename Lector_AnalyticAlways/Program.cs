

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace Lector_AnalyticAlways
{
    class Program
    {
        static void Main(string[] args)
        {

            
            string url = "https://interview2208.blob.core.windows.net/interview/Stock.CSV?sp=r&st=2020-01-07T06:09:04Z&se=2021-01-07T14:09:04Z&spr=https&sv=2019-02-02&sr=b&sig=A34uhCv1LATDR7XdeDy1OaZSOknZmjXKsf59j05GNfE%3D";
            string path_fichero = @"D:\Stock.csv";
            string cadConex = @"Data Source=DESKTOP-UU1EPGE\SQL_LOCAL;Initial Catalog=ANALYTICALWAYS;User Id=XXXXX;Password=XXXXXX";

            Dictionary<EnumImportadores, IImportador> importadores = null;

            try {

                importadores = _inicializarImportadores();

                
                if (_descargarCSV(url, path_fichero))
                {
                    //Descartado, TimeOut al cargar en memoria el CSV. Funcional con ficheros más pequeños.
                    //importadores[EnumImportadores.BulkConDataTable].Importar(path_fichero, cadConex);

                    //Descartado, Funcional pero con tiempo de ejecución no aceptable.
                    //importadores[EnumImportadores.LecturaSecuencialInsercion].Importar(path_fichero, cadConex);

                    //Solución adoptada. Volcado directo a tabla mediante procedimiento almacenado en BBDD.
                    importadores[EnumImportadores.PorPA].Importar(path_fichero, cadConex);


                    File.Delete(path_fichero);
                }

                else
                {

                    Console.WriteLine("Error al descargar el fichero CSV");
                }
            }
            catch (LectorAnalyticAlwaysException lAAExc)
            {
                Console.WriteLine(lAAExc.Message);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error no capturado en Program." + exc.Message + " --> " + exc.StackTrace);
            }

        }

        /// <summary>
        /// Inicialización de los distintos modos de importación
        /// </summary>
        /// <returns>Dictionary con los importadores instaciados.</returns>
        private static Dictionary<EnumImportadores, IImportador> _inicializarImportadores()
        {
            Dictionary<EnumImportadores, IImportador> importadores = new Dictionary<EnumImportadores, IImportador>();

            importadores.Add(EnumImportadores.BulkConDataTable, new ImportadorBulkDataTable());
            importadores.Add(EnumImportadores.LecturaSecuencialInsercion, new ImportadorLecturaSecuencialInsercion());
            importadores.Add(EnumImportadores.PorPA, new ImportadorPorPA());

            return importadores;
        }

        /// <summary>
        /// Método que descarga un fichero CSV de Azure desde la url facilitada para la prueba.
        /// </summary>
        /// <param name="url">Url de descarta</param>
        /// <param name="path_fichero">Path del fichero destino.</param>
        /// <returns>True en caso de descarga positiva, False en otro caso</returns>
        private static bool _descargarCSV(string url, string path_fichero)
        {
            bool resultado = false;

            try
            {
                var tareaDescarga = (new Microsoft.WindowsAzure.Storage.File.CloudFile(new Uri(url))).DownloadToFileAsync(path_fichero, System.IO.FileMode.Create);
                tareaDescarga.Wait();
                resultado = true;
            }
            catch (Exception exc)
            {
                throw new LectorAnalyticAlwaysException("Error al descargar el fichero CSV." + exc.Message + " --> " + exc.StackTrace);
            }
          



            return resultado;
        }
    }
}
