using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

namespace Compare_SZVSTAG_SZVM
{
    static class IOoperations
    {
        public const string katalogInINN = @"_In_INN";        //каталог для обрабатываемых файлов        
        //public const string katalogInUP = @"_In_UP";                                    //каталог для обрабатываемых файлов с УП        
        public const string katalogInCurators = @"_In_Curators";                        //каталог для обрабатываемых файлов с кодами кураторов (реестр)
        public const string katalogInCuratorsPartial = @"_In_Curators_partial";         //каталог для обрабатываемых файлов с кодами кураторов (несколько спец-в на один район)        

        public const string katalogOut = @"C:\_Out";                    //каталог для результирующих файлов         

        private static string errorLog = @"errorLog.txt";  //лог с ошибками обработки         



        //------------------------------------------------------------------------------------------
        //создаем каталог
        public static void DirectoryCreater(string createDirectoryName)
        {
            //Создаем пустой каталог
            if (!Directory.Exists(createDirectoryName))
                Directory.CreateDirectory(createDirectoryName);
        }

        //------------------------------------------------------------------------------------------
        //удаляем каталог
        private static void DirectoryDelete(string deleteDirectoryName)
        {
            try
            {
                //Удаляем каталог со всем содержимым 
                if (Directory.Exists(deleteDirectoryName))
                    Directory.Delete(deleteDirectoryName, true);
            }
            catch (IOException ex)
            {
                WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.WriteLine(new string('-', 17));
                Console.WriteLine("Внимание! Ошибка достаупа к каталогу \"{0}\" .", deleteDirectoryName);
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
            catch (Exception ex)
            {
                WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.WriteLine(new string('-', 17));                
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
        }

        //------------------------------------------------------------------------------------------
        //Создаем каталоги по умолчанию, очищаем временные каталоги
        public static void BasicDirectoryAndFileCreate()
        {
            
            //DirectoryCreater(katalogInUP);
            DirectoryCreater(katalogInCurators);
            DirectoryCreater(katalogInCuratorsPartial);           
            DirectoryCreater(katalogOut);
            DirectoryCreater(katalogInINN);
        }

        //------------------------------------------------------------------------------------------
        //Удаляем файл
        public static void FileDelete(string fileName)
        {
            //Удаляем файл
            try
            {
                File.Delete(fileName);                
            }            
            catch (Exception ex)
            {
                WriteLogError(ex.ToString());                
            }

        }

        //------------------------------------------------------------------------------------------       
        //Пишем ошибки в лог-файл, по умолчанию @"errorLog.txt"
        public static void WriteLogError(string errormessage)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(
                                                Environment.NewLine + "Внимание! В ходе выполнения операции произошли ошибки!"
                                                + Environment.NewLine + "Дополнительня информация отражена в файле errorLog.txt");

            //Console.WriteLine();
            //Console.WriteLine(new string('-', 17));
            //Console.WriteLine(errormessage.ToString());
            //Console.WriteLine(new string('-', 17));

            Console.ForegroundColor = ConsoleColor.Gray;

            try
            {
                using (StreamWriter writer = new StreamWriter(errorLog, true, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(new string('-', 17));
                    writer.WriteLine(DateTime.Now);
                    writer.WriteLine(new string('-', 17));
                    writer.WriteLine(errormessage);
                    writer.WriteLine(new string('-', 17));
                }

            }
            catch (IOException ex)
            {
                Console.WriteLine();
                Console.WriteLine(new string('-', 17));
                Console.WriteLine("Внимание! Ошибка достаупа к лог-файлу \"errorLog.txt\"");
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine(new string('-', 17));                
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
        }

        //------------------------------------------------------------------------------------------       
        //Формируем результирующий файл статистики
        public static void WriteLogs(string resultFile, string zagolovok, List<string> listData)
        {
            //формируем результирующий файл статистики
            using (StreamWriter writer = new StreamWriter(resultFile, false, Encoding.GetEncoding(1251)))
            {
                writer.WriteLine(zagolovok);

                foreach (var item in listData)
                {
                    writer.WriteLine(item);
                }
            }
        }

        //------------------------------------------------------------------------------------------        
        //Формируем результирующий файл из результатов сверки
        public static void CreateExportFile(string zagolovok, IEnumerable<string> listData, string nameFile)
        {
            try
            {
                //Добавляем в файл данные                
                using (StreamWriter writer = new StreamWriter(nameFile, true, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(zagolovok);

                    foreach (string item in listData)
                    {
                        writer.WriteLine(item.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.WriteLine(new string('-', 17));
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
        }

        //------------------------------------------------------------------------------------------       
        //Формируем результирующий файл
        public static void CreateExportFile_except(string zagolovok, string resultFile, SortedSet<string> sortedSetData)
        {
            try
            {
                //Добавляем в файл данные                
                using (StreamWriter writer = new StreamWriter(resultFile, true, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(zagolovok);

                    foreach (string item in sortedSetData)
                    {
                        writer.WriteLine(item.ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.WriteLine(new string('-', 17));
                Console.WriteLine(ex.ToString());
                Console.WriteLine(new string('-', 17));
            }
        }

    }
}
