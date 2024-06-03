using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;

namespace Compare_SZVSTAG_SZVM
{
    class Program
    {
        private static DateTime start;

        public static string raionS = "001";
        public static string raionPo = "034";

        public static string otchYear = "0";
        public static string p_date_priem_st = "0";
        public static string p_date_priem_fn = "0";

        //public static string regNum = "42000000000";
        //public static string inn = "000000000";

        public static Dictionary<string, DataFromRKASVDB> dictionary_dataFromPKASVDB = new Dictionary<string, DataFromRKASVDB>();       //Коллекция данных из БД РК АСВ

        public static SortedSet<string> sortedSet_RegNom_For_Select = new SortedSet<string>();          //Коллекция регНомеров из файла для запросов к БД

        //Коллекция уник СНИЛС из БД Perso (реестр ИСХД форм СЗВ-М) без ОТМН форм   KEY (INN + SNILS + KPP)
        public static Dictionary<string, DataFromPersoDB> uniqSNILS_ISXD_SZV_M_no_OTMN = new Dictionary<string, DataFromPersoDB>();
        public static HashSet<string> hashSet_SZVM_UniqSNILS = new HashSet<string>();         // массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ

        //Коллекция уник СНИЛС из БД Perso (реестр ИСХД форм СЗВ-СТАЖ) без ОТМН форм   KEY (INN + SNILS + KPP)
        public static Dictionary<string, DataFromPersoDB> uniqSNILS_ISXD_SZV_STAG_no_OTMN = new Dictionary<string, DataFromPersoDB>();
        public static HashSet<string> hashSet_SZV_STAG_UniqSNILS = new HashSet<string>();     // массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-М

        public static Dictionary<string, DataFromPersoDB> Compare_SZV_M = new Dictionary<string, DataFromPersoDB>();
        public static Dictionary<string, DataFromPersoDB> Compare_SZV_STAG = new Dictionary<string, DataFromPersoDB>();

        public static Dictionary<string, DataFromPersoDB> Compare_SZV_M_sverka2 = new Dictionary<string, DataFromPersoDB>();
        public static Dictionary<string, DataFromPersoDB> Compare_SZV_STAG_sverka2 = new Dictionary<string, DataFromPersoDB>();



        public static string zagolovokPKASVDB = "№ п/п" + ";" + "РегНомер" + ";" + "ИНН" + ";" + "КПП" + ";" + "Наименование" + ";" + "Куратор" + ";";

        public static string zagolovokPersoSZV = "№ п/п" + ";" + "Район" + ";" + "Наименование" + ";" + "РегНомер" + ";" + "СНИЛС" + ";" + "ИНН" + ";"
                                                    + "КПП" + ";" + "ОтчГод" + ";" + "ОтчМесяц" + ";" + "Куратор" + ";";



        public static SortedSet<string> sortedSet_INN = new SortedSet<string>();                     //Коллекция INN из файла





        static void Main(string[] args)
        {
            try
            {
                Console.SetWindowSize(105, 39);  //Устанавливаем размер окна консоли            

                NameValueCollection allAppSettings = ConfigurationManager.AppSettings;              //формируем массив настроек приложения                        
                string destinationfolderName = allAppSettings["destinationfolderName"];             //каталог назначения  

                //время начала обработки
                start = DateTime.Now;

                //Создаем каталоги по умолчанию
                IOoperations.BasicDirectoryAndFileCreate();



                //------------------------------------------------------------------------------------------
                //0. Вводим параметры

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(new string('-', 79));
                Console.WriteLine("Введите необходимые параметры:");
                Console.WriteLine(new string('-', 79));
                Console.ForegroundColor = ConsoleColor.Gray;



                Program.otchYear = "2020";
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.Write("Введите данные \"Отчетный период - год\" (по умолчанию - 2020): ");
                Console.ForegroundColor = ConsoleColor.Gray;
                string tmp_p_god = Console.ReadLine();
                if (tmp_p_god != "")
                {
                    Program.otchYear = tmp_p_god;
                }

                Console.WriteLine();
                Program.p_date_priem_st = "01.01.2018";
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.Write("Введите данные \"Дата приема с\" (по умолчанию - 01.01.2018): ");
                Console.ForegroundColor = ConsoleColor.Gray;
                string tmpReadLine = Console.ReadLine();
                if (tmpReadLine != "")
                {
                    Program.p_date_priem_st = tmpReadLine;
                }

                Console.WriteLine();
                Program.p_date_priem_fn = DateTime.Now.ToShortDateString();  //текущая системная дата
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.Write("Введите данные \"Дата приема по\" (по умолчанию - текущая системная дата): ");
                Console.ForegroundColor = ConsoleColor.Gray;
                tmpReadLine = Console.ReadLine();
                Console.WriteLine();
                if (tmpReadLine != "")
                {
                    Program.p_date_priem_fn = tmpReadLine;
                }


                //------------------------------------------------------------------------------------------
                //0.1. Выбираем данные (каталог \"_In_INN\")

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(new string('-', 79));
                Console.WriteLine("Обработка файлов в каталоге \"_In_INN\", пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                Console.WriteLine(new string('-', 79));
                Console.ForegroundColor = ConsoleColor.Gray;

                SelectDataFromINNFiles.ObrFileFromDirectory(IOoperations.katalogInINN);

                Console.WriteLine("Количество выбранных записей (каталог \"_In_INN\"): {0}", Program.sortedSet_INN.Count());



                if (Program.sortedSet_INN.Count() != 0)
                {
                    //------------------------------------------------------------------------------------------
                    //0.2. Выбираем данные по кураторам (каталог \"_In_Curators\")

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(new string('-', 79));
                    Console.WriteLine("Обработка файлов в каталоге \"_In_Curators\", пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    Console.WriteLine(new string('-', 79));
                    Console.ForegroundColor = ConsoleColor.Gray;

                    SelectDataFromCuratorsFile.ObrFileFromDirectory(IOoperations.katalogInCurators);


                    //TODO: Закомментировал "кураторы частично"
                    ////------------------------------------------------------------------------------------------
                    ////0.3. Выбираем данные по кураторам (каталог \"_In_Curators_partial\")

                    //Console.ForegroundColor = ConsoleColor.Cyan;
                    //Console.WriteLine(new string('-', 79));
                    //Console.WriteLine("Обработка файлов в каталоге \"_In_Curators_partial\", пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    //Console.WriteLine(new string('-', 79));
                    //Console.ForegroundColor = ConsoleColor.Gray;

                    //SelectDataFromCuratorsFilePartial.ObrFileFromDirectory(IOoperations.katalogInCuratorsPartial);






                    /*
                    var groups = Program.sortedSet_INN.Select((i, index) => new
                    {
                        i,
                        index
                    }).GroupBy(group => group.index / 10, element => element.i);




                    foreach (var group in groups)
                    {
                        Console.WriteLine("Group: {0}", group.Key);

                        foreach (var item in group)
                        {
                            Console.WriteLine("\tValue: {0}", item);
                        }
                    }

                    */

                    //пакетная обработка
                    //foreach (var item_inn in partINN)
                    //{
                    //}


                    //------------------------------------------------------------------------------------------
                    //1. Выбираем данные из РК АСВ            

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(new string('-', 79));
                    Console.WriteLine("Выбираем данные из РК АСВ, пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    Console.WriteLine(new string('-', 79));
                    Console.ForegroundColor = ConsoleColor.Gray;

                    //Выбираем данные из РК АСВ
                    SelectDataFromRKASVDB.SelectDataFromRKASV(DatabaseQueries.CreatequeryRKASV(Program.sortedSet_INN));

                    string nameResultFile_SelectFromASV = IOoperations.katalogOut + @"\" + @"_1_Данные_из_РК_АСВ_" + DateTime.Now.ToShortDateString() + ".csv";
                    if (File.Exists(nameResultFile_SelectFromASV)) { File.Delete(nameResultFile_SelectFromASV); }

                    //Формируем результирующий файл на основании данных из БД                
                    SelectDataFromRKASVDB.CreateExportFile(Program.zagolovokPKASVDB, Program.dictionary_dataFromPKASVDB, nameResultFile_SelectFromASV);



                    //------------------------------------------------------------------------------------------                
                    //2. Преобразуем регНомера в формат для запросов к БД
                    foreach (var item in Program.dictionary_dataFromPKASVDB)
                    {
                        Program.sortedSet_RegNom_For_Select.Add(SelectDataFromRKASVDB.ConvertRegNomForSelect(item.Key));
                    }



                    //------------------------------------------------------------------------------------------
                    //3. Выбор данных из БД Perso (СЗВ-М)
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(new string('-', 79));
                    Console.WriteLine("Выбор данных из БД Perso (СЗВ-М), пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    Console.WriteLine(new string('-', 79));
                    Console.ForegroundColor = ConsoleColor.Gray;


                    //3.1. Выбираем данные из БД Perso ISX                           
                    SelectDataFromPersoDB_SZVM.SelectDataFromPerso_ISXD(DatabaseQueries.CreateQueryPersoSZVM_ISX());

                    //3.2. Выбираем данные из БД Perso OTMN и сравниваем ИСХД и ОТМН формы, оставляя уник. СНИЛС
                    SelectDataFromPersoDB_SZVM.SelectDataFromPerso_OTMN(DatabaseQueries.CreateQueryPersoSZVM_OTMN());

                    //3.3 Сверяем полученные реестры и убираем отмененные формы 
                    //и формируем массив Program.hashSet_SZVM_UniqSNILS - уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ
                    SelectDataFromPersoDB_SZVM.Compare_SZV_M_ISX_and_OTMN();



                    //------------------------------------------------------------------------------------------
                    //4. Выбор данных из БД Perso (СЗВ-СТАЖ)

                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(new string('-', 79));
                    Console.WriteLine("Выбор данных из БД Perso (СЗВ-СТАЖ), пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    Console.WriteLine(new string('-', 79));
                    Console.ForegroundColor = ConsoleColor.Gray;

                    //4.1 Выбираем данные из БД Perso (СЗВ-СТАЖ, СЗВ-КОРР ИСХД формы)
                    SelectDataFromPersoDB_SZV_STAG.SelectDataFromPersoDB_ISXD(DatabaseQueries.CreateQueryPersoSZVSTAG_ISXD());

                    //4.2 Выбираем данные из БД Perso (СЗВ-СТАЖ ОТМН формы)
                    SelectDataFromPersoDB_SZV_STAG.SelectDataFromPersoDB_OTMN(DatabaseQueries.CreateQueryPersoSZVSTAG_OTMN());


                    //4.3 Сверяем полученные реестры и убираем отмененные формы
                    //и формируем массив Program.hashSet_SZV_STAG_UniqSNILS - уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-М
                    SelectDataFromPersoDB_SZV_STAG.Compare_SZV_STAG_ISX_and_OTMN();



                    //------------------------------------------------------------------------------------------
                    //5. Сравниваем коллекции СНИЛС в СЗВ-М и в СЗВ-СТАЖ и выбираем уникальных
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(new string('-', 100));
                    Console.WriteLine("Сравниваем коллекции СНИЛС в СЗВ-М и в СЗВ-СТАЖ и выбираем уникальных, пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                    Console.WriteLine(new string('-', 100));
                    Console.ForegroundColor = ConsoleColor.Gray;

                    Compare_SZVM_SZVSTAG.Compare();


                    //конец пакетной обработки
                    //foreach (var item_inn in partINN)
                    //{
                    //}






                    //------------------------------------------------------------------------------------------
                    string nameResultFile_Compare_SZV_M_ISX_and_OTMN = IOoperations.katalogOut + @"\" + @"_2_СЗВМ_УНИК_СНИЛС_" + DateTime.Now.ToShortDateString() + ".csv";
                    if (File.Exists(nameResultFile_Compare_SZV_M_ISX_and_OTMN)) { File.Delete(nameResultFile_Compare_SZV_M_ISX_and_OTMN); }

                    if (Program.uniqSNILS_ISXD_SZV_M_no_OTMN.Count() != 0)
                    {


                        //Формируем результирующий файл на основании данных из БД                
                        Compare_SZVM_SZVSTAG.CreateExportFile(nameResultFile_Compare_SZV_M_ISX_and_OTMN, Program.zagolovokPersoSZV, Program.uniqSNILS_ISXD_SZV_M_no_OTMN);

                    }



                    //------------------------------------------------------------------------------------------
                    string nameResultFile_Compare_SZV_STAG_ISX_and_OTMN = IOoperations.katalogOut + @"\" + @"_2_СЗВСТАЖ_УНИК_СНИЛС_" + DateTime.Now.ToShortDateString() + ".csv";
                    if (File.Exists(nameResultFile_Compare_SZV_STAG_ISX_and_OTMN)) { File.Delete(nameResultFile_Compare_SZV_STAG_ISX_and_OTMN); }

                    if (Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN.Count() != 0)
                    {


                        //Формируем результирующий файл на основании данных из БД                
                        Compare_SZVM_SZVSTAG.CreateExportFile(nameResultFile_Compare_SZV_STAG_ISX_and_OTMN, Program.zagolovokPersoSZV, Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN);

                    }



                    //------------------------------------------------------------------------------------------
                    //Создаем имя результирующего файла
                    string nameResultFile_CompareData_SZV = IOoperations.katalogOut + @"\" + @"_3_ЕСТЬ_В_СЗВ-СТАЖ_НЕТ_В_СЗВ-М_" + DateTime.Now.ToShortDateString() + ".csv";
                    //Лучше использовать проверку наличия файла                
                    if (File.Exists(nameResultFile_CompareData_SZV)) { File.Delete(nameResultFile_CompareData_SZV); }

                    if (Program.Compare_SZV_STAG.Count() != 0)
                    {
                        //------------------------------------------------------------------------------------------
                        //5.1 Создаем результирующий файл
                        Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-СТАЖ\"): {0} ", Program.Compare_SZV_STAG.Count());



                        //Формируем результирующий файл на основании данных из БД                     
                        Compare_SZVM_SZVSTAG.CreateExportFile(nameResultFile_CompareData_SZV, Program.zagolovokPersoSZV, Program.Compare_SZV_STAG);

                    }
                    else
                    {
                        //Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Уникальных СНИЛС в СЗВ-СТАЖ нет.");
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }



                    //------------------------------------------------------------------------------------------
                    //Создаем имя результирующего файла
                    string nameResultFile_CompareData_SZV_M = IOoperations.katalogOut + @"\" + @"_3_ЕСТЬ_В_СЗВ-М_НЕТ_В_СЗВ-СТАЖ_" + DateTime.Now.ToShortDateString() + ".csv";
                    //Лучше использовать проверку наличия файла                
                    if (File.Exists(nameResultFile_CompareData_SZV_M)) { File.Delete(nameResultFile_CompareData_SZV_M); }

                    if (Program.Compare_SZV_M.Count() != 0)
                    {
                        //------------------------------------------------------------------------------------------
                        //5.2 Создаем результирующий файл
                        Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-М\"): {0} ", Program.Compare_SZV_M.Count());



                        //Формируем результирующий файл на основании данных из БД                     
                        Compare_SZVM_SZVSTAG.CreateExportFile(nameResultFile_CompareData_SZV_M, Program.zagolovokPersoSZV, Program.Compare_SZV_M);

                    }
                    else
                    {
                        //Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Уникальных СНИЛС в СЗВ-М нет.");
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }

                    Console.WriteLine(new string('-', 79));

                    //------------------------------------------------------------------------------------------
                    //Создаем имя результирующего файла
                    string nameResultFile_CompareData_SZV_sverka2 = IOoperations.katalogOut + @"\" + @"_5_ЕСТЬ_В_СЗВ-СТАЖ_НЕТ_В_СЗВ-М___по_ИНН___" + DateTime.Now.ToShortDateString() + ".csv";
                    //Лучше использовать проверку наличия файла                
                    if (File.Exists(nameResultFile_CompareData_SZV_sverka2)) { File.Delete(nameResultFile_CompareData_SZV_sverka2); }

                    if (Program.Compare_SZV_STAG_sverka2.Count() != 0)
                    //if (Compare_SZVM_SZVSTAG.hashsetUniqSZV_STAG_sverka2_tmp.Count() != 0)
                    {
                        //------------------------------------------------------------------------------------------
                        //6.1 Создаем результирующий файл
                        //Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-СТАЖ\", сверка по ИНН): {0} ", Compare_SZVM_SZVSTAG.hashsetUniqSZV_STAG_sverka2_tmp.Count());
                        Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-СТАЖ\", сверка по ИНН): {0} ", Program.Compare_SZV_STAG_sverka2.Count());


                        //Формируем результирующий файл на основании данных из БД                     
                        Compare_SZVM_SZVSTAG.CreateExportFile_sverka2(nameResultFile_CompareData_SZV_sverka2, Program.zagolovokPersoSZV, Program.Compare_SZV_STAG_sverka2);
                        //Compare_SZVM_SZVSTAG.CreateExportFileHashSet(nameResultFile_CompareData_SZV_sverka2, "№;INN;SNILS;otchMonth;", Compare_SZVM_SZVSTAG.hashsetUniqSZV_STAG_sverka2_tmp);

                    }
                    else
                    {
                        //Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Уникальных СНИЛС в СЗВ-СТАЖ нет (сверка по ИНН).");
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }



                    //------------------------------------------------------------------------------------------
                    //Создаем имя результирующего файла
                    string nameResultFile_CompareData_SZV_M_sverka2 = IOoperations.katalogOut + @"\" + @"_5_ЕСТЬ_В_СЗВ-М_НЕТ_В_СЗВ-СТАЖ___по_ИНН___" + DateTime.Now.ToShortDateString() + ".csv";
                    //Лучше использовать проверку наличия файла                
                    if (File.Exists(nameResultFile_CompareData_SZV_M_sverka2)) { File.Delete(nameResultFile_CompareData_SZV_M_sverka2); }

                    
                    if (Program.Compare_SZV_M_sverka2.Count() != 0)
                    //if (Compare_SZVM_SZVSTAG.hashsetUniqSZVM_sverka2_tmp.Count() != 0)
                    {
                        //------------------------------------------------------------------------------------------
                        //6.2 Создаем результирующий файл
                        //Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-М\", сверка по ИНН): {0} ", Compare_SZVM_SZVSTAG.hashsetUniqSZVM_sverka2_tmp.Count());
                        Console.WriteLine("Количество выбранных строк (\"уникальных в СЗВ-М\", сверка по ИНН): {0} ", Program.Compare_SZV_M_sverka2.Count());


                        //Формируем результирующий файл на основании данных из БД                     
                        Compare_SZVM_SZVSTAG.CreateExportFile_sverka2(nameResultFile_CompareData_SZV_M_sverka2, Program.zagolovokPersoSZV, Program.Compare_SZV_M_sverka2);
                        //Compare_SZVM_SZVSTAG.CreateExportFileHashSet(nameResultFile_CompareData_SZV_M_sverka2, "№;INN;SNILS;otchMonth;", Compare_SZVM_SZVSTAG.hashsetUniqSZVM_sverka2_tmp);

                    }
                    else
                    {
                        //Console.WriteLine();
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Уникальных СНИЛС в СЗВ-М нет (сверка по ИНН).");
                        Console.ForegroundColor = ConsoleColor.Gray;
                    }

                    Console.WriteLine();
                }





                //TODO: !!! Выбрать "Наименование" из РК АСВ   + "Куратор" + ";";





                //Вычисляем время затраченное на обработку
                TimeSpan stop = DateTime.Now - start;

                //Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(new string('-', 79));
                Console.WriteLine("Обработка выполнилась за " + stop.TotalSeconds + " сек. ({0})", DateTime.Now.ToLongTimeString());
                Console.ForegroundColor = ConsoleColor.Gray;

                Console.ReadKey();

                //Задержка экрана
                //Thread.Sleep(TimeSpan.FromSeconds(5));









                /*
                
                */






                /*
                //------------------------------------------------------------------------------------------
                //0.1. Выбираем данные (каталог \"_In_RegNom\")

                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine(new string('-', 107));
                Console.WriteLine("Обработка файлов в каталоге \"_In_RegNom\", пожалуйста ждите... ({0})", DateTime.Now.ToLongTimeString());
                Console.WriteLine(new string('-', 107));
                Console.ForegroundColor = ConsoleColor.Gray;

                SelectDataFromRegNomFiles.ObrFileFromDirectory(IOoperations.katalogInRegNom);

                Console.WriteLine();
                Console.WriteLine("Количество выбранных записей (каталог \"_In_RegNom\"): {0}", Program.sortedSet_RegNom.Count());

                //Преобразуем регНомера в формат для запросов к БД
                foreach (var item in Program.sortedSet_RegNom)
                {
                    Program.sortedSet_RegNom_For_Select.Add(SelectDataFromRegNomFiles.ConvertRegNomForSelect(item));
                }
                */





            }
            catch (Exception ex)
            {
                IOoperations.WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ForegroundColor = ConsoleColor.Gray;
            }
        }
    }
}
