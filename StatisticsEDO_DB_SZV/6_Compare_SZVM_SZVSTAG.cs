using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using System.Xml;
using System.Xml.Linq;

namespace Compare_SZVSTAG_SZVM
{

    static class Compare_SZVM_SZVSTAG
    {
        public static HashSet<string> hashsetUniqSZVM_sverka2_tmp;
        public static HashSet<string> hashsetUniqSZV_STAG_sverka2_tmp;


        //------------------------------------------------------------------------------------------
        public static void Compare()
        {
            try
            {
                //1. Временная коллекция СЗВ-М
                HashSet<string> hashsetUniqSZVM = new HashSet<string>(Program.hashSet_SZVM_UniqSNILS);



                //------------------------------------------------------------------------------------------
                //1.1 Создаем коллекцию СЗВМ   KEY (INN + SNILS otchMonth) для параллельной сверки (без КПП, реорганизация, перерегистрация)
                HashSet<string> hashsetUniqSZVM_sverka2 = new HashSet<string>();
                foreach (var item in hashsetUniqSZVM)
                {
                    char[] separator = { ',' };    //список разделителей в строке
                    string[] massiveStr = item.Split(separator);     //создаем массив из строк между разделителями

                    //Формируем массив уникальных hashsetUniqSZVM   KEY (INN + SNILS + KPP + otchMonth)
                    hashsetUniqSZVM_sverka2.Add(massiveStr[0]+ ";" + massiveStr[1] + ";" + massiveStr[3]);
                }
                //------------------------------------------------------------------------------------------




                //2. Временная коллекция СЗВ-СТАЖ
                HashSet<string> hashsetUniqSZV_STAG = new HashSet<string>(Program.hashSet_SZV_STAG_UniqSNILS);



                //------------------------------------------------------------------------------------------
                //2.1 Создаем коллекцию СЗВ-СТАЖ   KEY (INN + SNILS otchMonth) для параллельной сверки (без КПП, реорганизация, перерегистрация)
                HashSet<string> hashsetUniqSZV_STAG_sverka2 = new HashSet<string>();
                foreach (var item in hashsetUniqSZV_STAG)
                {
                    char[] separator = { ',' };    //список разделителей в строке
                    string[] massiveStr = item.Split(separator);     //создаем массив из строк между разделителями

                    //Формируем массив уникальных hashsetUniqSZV_STAG   KEY (INN + SNILS + KPP + otchMonth)
                    hashsetUniqSZV_STAG_sverka2.Add(massiveStr[0] + ";" + massiveStr[1] + ";" + massiveStr[3]);
                }
                //------------------------------------------------------------------------------------------


                //3. Сравниваем коллекции и находим уникальные в СЗВ-М
                hashsetUniqSZVM.ExceptWith(Program.hashSet_SZV_STAG_UniqSNILS);

                if (hashsetUniqSZVM.Count != 0)
                {
                    foreach (var keySZVM in hashsetUniqSZVM)
                    {
                        char[] separator = { ',' };    //список разделителей в строке
                        string[] massiveStr = keySZVM.Split(separator);     //создаем массив из строк между разделителями

                        //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ

                        //public DataFromPersoDB(string regNum = "", string strnum = "",
                        //        string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                        //        DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))


                        if (massiveStr.Count() == 4)
                        {
                            //v1
                            //Program.Compare_SZV_M[massiveStr[0] + massiveStr[1] + massiveStr[2] + massiveStr[3]] = new DataFromPersoDB(
                            //                                          //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2] + massiveStr[3]].regNum,
                            //                                          Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                            //                                          massiveStr[1],
                            //                                          massiveStr[0],
                            //                                          massiveStr[2],
                            //                                          //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2] + massiveStr[3]].otchYear,
                            //                                          Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,
                            //                                          massiveStr[3]);

                            //v2 (периоды в одну строку)
                            DataFromPersoDB tmpData_Compare_SZVM = new DataFromPersoDB();
                            if (Program.Compare_SZV_M.TryGetValue(massiveStr[0] + massiveStr[1] + massiveStr[2], out tmpData_Compare_SZVM))
                            {
                                Program.Compare_SZV_M[massiveStr[0] + massiveStr[1] + massiveStr[2]] =
                                                            new DataFromPersoDB(
                                                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                                massiveStr[2],
                                                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,

                                                                AddOtchMonth(
                                                                     Program.Compare_SZV_M[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchMonth,
                                                                    massiveStr[3]
                                                                    )
                                                                                );
                            }
                            else
                            {
                                Program.Compare_SZV_M[massiveStr[0] + massiveStr[1] + massiveStr[2]] =
                                                            new DataFromPersoDB(
                                                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                                massiveStr[2],
                                                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,

                                                                AddOtchMonth(
                                                                     "",
                                                                    massiveStr[3]
                                                                    )
                                                                                );
                            }


                            //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]];
                        }
                        else
                        {
                            continue;
                        }
                    }

                }



                //4. Сравниваем коллекции и находим уникальные в СЗВ-СТАЖ
                hashsetUniqSZV_STAG.ExceptWith(Program.hashSet_SZVM_UniqSNILS);

                if (hashsetUniqSZV_STAG.Count != 0)
                {
                    foreach (var keySZV_STAG in hashsetUniqSZV_STAG)
                    {
                        char[] separator = { ',' };    //список разделителей в строке
                        string[] massiveStr = keySZV_STAG.Split(separator);     //создаем массив из строк между разделителями

                        if (massiveStr.Count() == 4)
                        {
                            //v1
                            //Program.Compare_SZV_STAG[massiveStr[0] + massiveStr[1] + massiveStr[2] + massiveStr[3]] =
                            //    new DataFromPersoDB(
                            //        Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                            //        massiveStr[1],
                            //        massiveStr[0],
                            //        massiveStr[2],
                            //        Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,
                            //        massiveStr[3]
                            //                       );


                            //v2 (периоды в одну строку)
                            DataFromPersoDB tmpData_Compare_SZV_STAG = new DataFromPersoDB();
                            if (Program.Compare_SZV_STAG.TryGetValue(massiveStr[0] + massiveStr[1] + massiveStr[2], out tmpData_Compare_SZV_STAG))
                            {
                                Program.Compare_SZV_STAG[massiveStr[0] + massiveStr[1] + massiveStr[2]] =
                                                            new DataFromPersoDB(
                                                                Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                                massiveStr[2],
                                                                Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,

                                                                AddOtchMonth(
                                                                     Program.Compare_SZV_STAG[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchMonth,
                                                                    massiveStr[3]
                                                                    )
                                                                                );
                            }
                            else
                            {
                                Program.Compare_SZV_STAG[massiveStr[0] + massiveStr[1] + massiveStr[2]] =
                                                            new DataFromPersoDB(
                                                                Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].regNum,
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                                massiveStr[2],
                                                                Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]].otchYear,

                                                                AddOtchMonth(
                                                                     "",
                                                                    massiveStr[3]
                                                                    )
                                                                                );
                            }
                        }
                        else
                        {
                            continue;
                        }

                        //Program.Compare_SZV_STAG[keySZV_STAG] = Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[keySZV_STAG];
                    }

                }



                //------------------------------------------------------------------------------------------                
                //5.1. Временная коллекция СЗВ-М (для 2го этапа сверки)   KEY (INN + SNILS otchMonth)
                //HashSet<string> hashsetUniqSZVM_sverka2_tmp = new HashSet<string>(hashsetUniqSZVM_sverka2);
                hashsetUniqSZVM_sverka2_tmp = new HashSet<string>(hashsetUniqSZVM_sverka2);

                //5.2. Временная коллекция СЗВ-СТАЖ (для 2го этапа сверки)   KEY (INN + SNILS otchMonth)
                //HashSet<string> hashsetUniqSZV_STAG_sverka2_tmp = new HashSet<string>(hashsetUniqSZV_STAG_sverka2);
                hashsetUniqSZV_STAG_sverka2_tmp = new HashSet<string>(hashsetUniqSZV_STAG_sverka2);



                //5.3 Сравниваем коллекции и находим уникальные в СЗВ-М
                hashsetUniqSZVM_sverka2_tmp.ExceptWith(hashsetUniqSZV_STAG_sverka2);

                if (hashsetUniqSZVM_sverka2_tmp.Count != 0)
                {
                    foreach (var keySZVM in hashsetUniqSZVM_sverka2_tmp)
                    {
                        char[] separator = { ';' };    //список разделителей в строке
                        string[] massiveStr = keySZVM.Split(separator);     //создаем массив из строк между разделителями

                        //Формируем массив уникальных KEY (INN + SNILS + otchMonth) для сверки с СЗВ-СТАЖ

                        //public DataFromPersoDB(string regNum = "", string strnum = "",
                        //        string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                        //        DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))


                        if (massiveStr.Count() == 3)
                        {
                            //v2 (периоды в одну строку)
                            DataFromPersoDB tmpData_Compare_SZVM = new DataFromPersoDB();
                            if (Program.Compare_SZV_M_sverka2.TryGetValue(massiveStr[0] + massiveStr[1], out tmpData_Compare_SZVM))
                            {
                                Program.Compare_SZV_M_sverka2[massiveStr[0] + massiveStr[1]] =
                                                            new DataFromPersoDB(
                                                                "",
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                               "",
                                                                Program.otchYear,

                                                                AddOtchMonth(
                                                                    Program.Compare_SZV_M_sverka2[massiveStr[0] + massiveStr[1]].otchMonth,
                                                                    massiveStr[2]
                                                                    )
                                                                                );
                            }
                            else
                            {
                                Program.Compare_SZV_M_sverka2[massiveStr[0] + massiveStr[1]] =
                                                            new DataFromPersoDB(
                                                                "",
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                               "",
                                                                Program.otchYear,

                                                                AddOtchMonth(
                                                                    "",
                                                                    massiveStr[2]
                                                                    )
                                                                                );
                            }


                            //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[massiveStr[0] + massiveStr[1] + massiveStr[2]];
                        }
                        else
                        {
                            continue;
                        }
                    }

                }



                //5.4 Сравниваем коллекции и находим уникальные в СЗВ-СТАЖ
                hashsetUniqSZV_STAG_sverka2_tmp.ExceptWith(hashsetUniqSZVM_sverka2);

                if (hashsetUniqSZV_STAG_sverka2_tmp.Count != 0)
                {
                    foreach (var keySZVM in hashsetUniqSZV_STAG_sverka2_tmp)
                    {
                        char[] separator = { ';' };    //список разделителей в строке
                        string[] massiveStr = keySZVM.Split(separator);     //создаем массив из строк между разделителями

                        //Формируем массив уникальных KEY (INN + SNILS otchMonth) для сверки с СЗВ-М

                        //public DataFromPersoDB(string regNum = "", string strnum = "",
                        //        string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                        //        DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))


                        if (massiveStr.Count() == 3)
                        {


                            //v2 (периоды в одну строку)
                            DataFromPersoDB tmpData_Compare_SZV_STAG = new DataFromPersoDB();
                            if (Program.Compare_SZV_STAG_sverka2.TryGetValue(massiveStr[0] + massiveStr[1], out tmpData_Compare_SZV_STAG))
                            {
                                Program.Compare_SZV_STAG_sverka2[massiveStr[0] + massiveStr[1]] =
                                                            new DataFromPersoDB(
                                                                "",
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                               "",
                                                                Program.otchYear,

                                                                AddOtchMonth(
                                                                    Program.Compare_SZV_STAG_sverka2[massiveStr[0] + massiveStr[1]].otchMonth,
                                                                    massiveStr[2]
                                                                    )
                                                                                );
                            }
                            else
                            {
                                Program.Compare_SZV_STAG_sverka2[massiveStr[0] + massiveStr[1]] =
                                                            new DataFromPersoDB(
                                                                "",
                                                                massiveStr[1],
                                                                massiveStr[0],
                                                               "",
                                                                Program.otchYear,

                                                                AddOtchMonth(
                                                                    "",
                                                                    massiveStr[2]
                                                                    )
                                                                                );
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }

                }












                Console.WriteLine();

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


        private static string AddOtchMonth(string otchMonth_old, string otchMonth_new)
        {
            //Создаем массив уникальных месяцев из dictionary_uniqSNILS_ISXD_SZV_STAG.Value.otchMonth
            HashSet<int> tmpStr = new HashSet<int>();

            char[] separator = { ',' };    //список разделителей в строке
            string[] massiveStr = otchMonth_old.Split(separator);     //создаем массив из строк между разделителями

            if (massiveStr.Count() != 0)
            {
                foreach (var item in massiveStr)
                {
                    if (item != "")
                    {
                        tmpStr.Add(Convert.ToInt32(item));
                    }
                }
            }



            //Добавляем новый период 
            tmpStr.Add(Convert.ToInt32(otchMonth_new));



            string monthCollection = "";
            int tmpStrCount = tmpStr.Count();

            if (tmpStrCount == 1)
            {
                foreach (var item in tmpStr)
                {
                    monthCollection = monthCollection + item + ",";
                }
            }
            else if (tmpStrCount > 1)
            {
                //Возвращаем строку из уникальных месяцев через запятую
                foreach (var item in tmpStr)
                {
                    --tmpStrCount;
                    if (tmpStrCount != 0)
                    {
                        monthCollection = monthCollection + item + ",";
                    }
                    else
                    {
                        monthCollection = monthCollection + item;
                    }
                }
            }
            else
            {
                monthCollection = "";
            }

            return monthCollection;

        }

        //------------------------------------------------------------------------------------------       
        //Формируем результирующий файл статистики
        public static void CreateExportFile(string resultFile, string zagolovok, Dictionary<string, DataFromPersoDB> dictionary_CompareData)
        {
            try
            {
                //формируем результирующий файл статистики
                using (StreamWriter writer = new StreamWriter(resultFile, false, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(zagolovok);

                    int i = 0;

                    foreach (var item in dictionary_CompareData)
                    {
                        i++;
                        writer.Write(i + ";");
                        writer.Write(SelectRaion(item.Value.regNum) + ";");
                        writer.Write(Program.dictionary_dataFromPKASVDB[item.Value.regNum].insurer_short_name + ";");
                        writer.Write(item.Value.ToString());
                        writer.WriteLine(Program.dictionary_dataFromPKASVDB[item.Value.regNum].kurator + ";");
                    }
                }
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

        public static void CreateExportFileHashSet(string resultFile, string zagolovok, HashSet<string> hashSet_CompareData)
        {
            try
            {
                //формируем результирующий файл статистики
                using (StreamWriter writer = new StreamWriter(resultFile, false, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(zagolovok);

                    int i = 0;

                    foreach (var item in hashSet_CompareData)
                    {
                        i++;
                        writer.Write(i + ";");                        
                        writer.WriteLine(item.ToString() + ";");
                    }
                }
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

        public static void CreateExportFile_sverka2(string resultFile, string zagolovok, Dictionary<string, DataFromPersoDB> dictionary_CompareData)
        {
            try
            {
                //формируем результирующий файл статистики
                using (StreamWriter writer = new StreamWriter(resultFile, false, Encoding.GetEncoding(1251)))
                {
                    writer.WriteLine(zagolovok);

                    int i = 0;

                    foreach (var item in dictionary_CompareData)
                    {
                        i++;
                        writer.Write(i + ";");
                        writer.Write("" + ";");
                        writer.Write("" + ";");
                        writer.Write(item.Value.ToString());
                        writer.WriteLine("" + ";");
                    }
                }
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

        private static string SelectRaion(string regNum)
        {
            try
            {
                if (regNum.Count() == 14)
                {
                    return "042-0" + regNum[5] + regNum[6];
                }
                else
                {
                    return "";
                }

            }
            catch (Exception ex)
            {
                IOoperations.WriteLogError(ex.ToString());

                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ForegroundColor = ConsoleColor.Gray;

                return "";
            }
        }

    }
}