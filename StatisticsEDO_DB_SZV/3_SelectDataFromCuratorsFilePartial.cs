//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//using System.IO;
//using System.Xml;
//using System.Xml.Linq;

//namespace Compare_SZVSTAG_SZVM
//{
//    #region считанная из файла информация

//    public class DataFromCuratorsFilePartial
//    {
//        public string regNom;
//        public string curator;

//        public DataFromCuratorsFilePartial(string regNumStrah = "", string curator = "")
//        {
//            this.regNom = regNumStrah;
//            this.curator = curator;
//        }


//        public override string ToString()
//        {
//            return regNom + ";" + curator + ";";
//        }
//    }

//    #endregion

//    //------------------------------------------------------------------------------------------
//    #region Выбор данных из файла
//    static class SelectDataFromCuratorsFilePartial
//    {
//        //Маска поиска файлов
//        private static string fileSearchMask = "*.csv";

//        public static Dictionary<string, DataFromCuratorsFilePartial> dictionary_CuratorsPartial = new Dictionary<string, DataFromCuratorsFilePartial>();           //Коллекция данных из файлов



//        //------------------------------------------------------------------------------------------
//        //Открываем поток для чтения из файла и выбираем нужные позиции           
//        private static void ReadAndParseTextFile(string openFile)
//        {
//            try
//            {
//                using (StreamReader reader = new StreamReader(openFile, Encoding.GetEncoding(1251)))
//                {
//                    while (!reader.EndOfStream)
//                    {
//                        string strTmp = reader.ReadLine();

//                        char[] separator = { ';', ',' };    //список разделителей в строке
//                        string[] massiveStr = strTmp.Split(separator);     //создаем массив из строк между разделителями

//                        if (massiveStr.Count() == 3 && massiveStr[1].Count() == 14 && massiveStr[1] != "" && massiveStr[1] != " " && massiveStr[2] != "" && massiveStr[2] != " ")
//                        {
//                            dictionary_CuratorsPartial.Add(massiveStr[1], new DataFromCuratorsFilePartial(massiveStr[1], massiveStr[2]));
//                        }
//                        else
//                        {
//                            continue;
//                        }
//                    }

//                    if (dictionary_CuratorsPartial.Count() == 0)
//                    {
//                        Console.WriteLine();
//                        Console.ForegroundColor = ConsoleColor.Red;
//                        Console.WriteLine("Внимание! Нет информации о кураторах.");
//                        Console.ForegroundColor = ConsoleColor.Gray;
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                IOoperations.WriteLogError(ex.ToString());

//                Console.WriteLine();
//                Console.ForegroundColor = ConsoleColor.Red;
//                Console.WriteLine(ex.Message);
//                Console.ForegroundColor = ConsoleColor.Gray;
//            }
//        }



//        //------------------------------------------------------------------------------------------        
//        /// <summary>
//        /// Выбираем данные из файлов
//        /// </summary>
//        /// <param name="folderIn">Каталог с обрабатываемыми файлами</param>
//        public static void ObrFileFromDirectory(string folderIn)
//        {
//            //Очищаем коллекции для данных из файлов
//            dictionary_CuratorsPartial.Clear();

//            DirectoryInfo dirInfo = new DirectoryInfo(folderIn);

//            if (dirInfo.GetFiles(fileSearchMask).Count() == 0)
//            {
//                Console.WriteLine();
//                Console.ForegroundColor = ConsoleColor.Red;
//                Console.WriteLine("Нет файлов для обработки.");
//                Console.ForegroundColor = ConsoleColor.Gray;
//            }
//            else
//            {
//                try
//                {
//                    //обрабатываем каждый файл по отдельности
//                    foreach (FileInfo file in dirInfo.GetFiles(fileSearchMask))
//                    {
//                        //Открываем поток для чтения из файла и выбираем нужные позиции   
//                        ReadAndParseTextFile(file.FullName);
//                    }

//                    //Console.WriteLine();
//                    Console.WriteLine("Количество выбранных записей о кураторах (каталог \"_In_Curators_partial\"): {0}", SelectDataFromCuratorsFilePartial.dictionary_CuratorsPartial.Count());
//                    //Console.WriteLine();
//                }
//                catch (Exception ex)
//                {
//                    IOoperations.WriteLogError(ex.ToString());

//                    Console.WriteLine();
//                    Console.ForegroundColor = ConsoleColor.Red;
//                    Console.WriteLine(ex.Message);
//                    Console.ForegroundColor = ConsoleColor.Gray;
//                }
//            }
//        }
//    }

//    #endregion

//}
