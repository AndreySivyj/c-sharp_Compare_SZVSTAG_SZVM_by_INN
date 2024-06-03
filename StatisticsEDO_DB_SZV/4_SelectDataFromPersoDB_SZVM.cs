using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Data.Common;
using IBM.Data.DB2;

namespace Compare_SZVSTAG_SZVM
{
    static class SelectDataFromPersoDB_SZVM
    {
        private static Dictionary<string, DataFromPersoDB> dictionary_uniqSNILS_ISXD_SZV_M = new Dictionary<string, DataFromPersoDB>();       //Коллекция данных
        private static Dictionary<string, DataFromPersoDB> dictionary_uniqSNILS_OTMN_SZV_M = new Dictionary<string, DataFromPersoDB>();       //Коллекция данных        

        async public static void SelectDataFromPerso_ISXD(string query_ISXD)
        {
            try
            {
                using (DB2Connection connection = new DB2Connection("Server=1.1.1.1:50000;Database=PERSDB;UID=regusr;PWD=password;"))
                {

                    //открываем соединение
                    await connection.OpenAsync();

                    DB2Command command_ISXD = connection.CreateCommand();
                    command_ISXD.CommandText = query_ISXD;

                    //Устанавливаем значение таймаута
                    command_ISXD.CommandTimeout = 570;

                    DbDataReader reader_ISXD = await command_ISXD.ExecuteReaderAsync();

                    //int i_ISXD = 0;

                    while (await reader_ISXD.ReadAsync())
                    {
                        // @"select distinct p.regnumb, w.strnum, w.god, w.period, s.DATE_INS, s.TIME_INS " 

                        //public DataFromPersoDB(string regNum = "", string strnum = "",
                        //        string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                        //        DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))



                        //регНом+СНИЛС есть в словаре
                        //KEY: regNum + INN + SNILS + KPP + otchMonth
                        DataFromPersoDB tmpData = new DataFromPersoDB();
                        if (dictionary_uniqSNILS_ISXD_SZV_M.TryGetValue(
                                                                        ConvertRegNom(reader_ISXD[0].ToString()) +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn +
                                                                        reader_ISXD[1].ToString() +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp +
                                                                        reader_ISXD[3].ToString(),
                                                                                                                                                                 out tmpData))
                        {
                            //сверяем даты импорта в БД (больше)
                            if (Convert.ToDateTime(reader_ISXD[4].ToString()) > tmpData.dateINS)
                            {
                                //KEY: INN + SNILS + KPP + otchMonth
                                dictionary_uniqSNILS_ISXD_SZV_M[
                                    ConvertRegNom(reader_ISXD[0].ToString()) +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn +
                                    reader_ISXD[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp +
                                    reader_ISXD[3].ToString()
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_ISXD[0].ToString()),
                                                                reader_ISXD[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp,
                                                                reader_ISXD[2].ToString(),
                                                                reader_ISXD[3].ToString(),
                                                                Convert.ToDateTime(reader_ISXD[4].ToString()),
                                                                Convert.ToDateTime(reader_ISXD[5].ToString())
                                                                                );
                            }
                            //сверяем даты импорта в БД (равны)
                            else if (Convert.ToDateTime(reader_ISXD[4].ToString()) == Convert.ToDateTime(tmpData.dateINS))
                            {
                                //тогда сверяем время импорта в БД (больше)
                                if (Convert.ToDateTime(reader_ISXD[5].ToString()) > tmpData.timeINS)
                                {
                                    dictionary_uniqSNILS_ISXD_SZV_M[
                                    ConvertRegNom(reader_ISXD[0].ToString()) +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn +
                                    reader_ISXD[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp +
                                    reader_ISXD[3].ToString()
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_ISXD[0].ToString()),
                                                                reader_ISXD[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp,
                                                                reader_ISXD[2].ToString(),
                                                                reader_ISXD[3].ToString(),
                                                                Convert.ToDateTime(reader_ISXD[4].ToString()),
                                                                Convert.ToDateTime(reader_ISXD[5].ToString())
                                                                                );
                                }
                                else
                                {
                                    continue;
                                }
                            }
                            else
                            {
                                continue;
                            }

                        }
                        else
                        {
                            dictionary_uniqSNILS_ISXD_SZV_M[
                                    ConvertRegNom(reader_ISXD[0].ToString()) +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn +
                                    reader_ISXD[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp +
                                    reader_ISXD[3].ToString()
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_ISXD[0].ToString()),
                                                                reader_ISXD[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp,
                                                                reader_ISXD[2].ToString(),
                                                                reader_ISXD[3].ToString(),
                                                                Convert.ToDateTime(reader_ISXD[4].ToString()),
                                                                Convert.ToDateTime(reader_ISXD[5].ToString())
                                                                                );
                        }

                        //i_ISXD++;
                    }
                    reader_ISXD.Close();

                    Console.WriteLine("Количество выбранных строк из БД Perso (ИСХД формы СЗВ-М): {0} ", dictionary_uniqSNILS_ISXD_SZV_M.Count());
                    //Console.WriteLine();

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

        async public static void SelectDataFromPerso_OTMN(string query_OTMN)
        {
            try
            {
                using (DB2Connection connection = new DB2Connection("Server=1.1.1.1:50000;Database=PERSDB;UID=regusr;PWD=password;"))
                {
                    //открываем соединение
                    await connection.OpenAsync();

                    DB2Command command = connection.CreateCommand();
                    command.CommandText = query_OTMN;

                    //Устанавливаем значение таймаута
                    command.CommandTimeout = 570;

                    DbDataReader reader = await command.ExecuteReaderAsync();

                    //int i_OTMN = 0;

                    while (await reader.ReadAsync())
                    {
                        //регНом+СНИЛС есть в словаре
                        DataFromPersoDB tmpData = new DataFromPersoDB();
                        if (dictionary_uniqSNILS_OTMN_SZV_M.TryGetValue(
                                                                        ConvertRegNom(reader[0].ToString()) +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn +
                                                                        reader[1].ToString() +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp +
                                                                        reader[3].ToString(),
                                                                                                                                                            out tmpData))
                        {
                            //сверяем даты импорта в БД (больше)
                            if (Convert.ToDateTime(reader[4].ToString()) > tmpData.dateINS)
                            {
                                dictionary_uniqSNILS_OTMN_SZV_M[
                                    ConvertRegNom(reader[0].ToString()) +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn +
                                    reader[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp +
                                    reader[3].ToString()
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader[0].ToString()),
                                                                reader[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp,
                                                                reader[2].ToString(),
                                                                reader[3].ToString(),
                                                                Convert.ToDateTime(reader[4].ToString()),
                                                                Convert.ToDateTime(reader[5].ToString())
                                                                                );
                            }
                            //сверяем даты импорта в БД (равны)
                            else if (Convert.ToDateTime(reader[4].ToString()) == tmpData.dateINS)
                            {
                                //тогда сверяем время импорта в БД (больше)
                                if (Convert.ToDateTime(reader[5].ToString()) > Convert.ToDateTime(tmpData.timeINS))
                                {
                                    dictionary_uniqSNILS_OTMN_SZV_M[
                                     ConvertRegNom(reader[0].ToString()) +
                                     Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn +
                                     reader[1].ToString() +
                                     Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp +
                                     reader[3].ToString()
                                                                ] =
                                                             new DataFromPersoDB(
                                                                ConvertRegNom(reader[0].ToString()),
                                                                reader[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp,
                                                                reader[2].ToString(),
                                                                reader[3].ToString(),
                                                                Convert.ToDateTime(reader[4].ToString()),
                                                                Convert.ToDateTime(reader[5].ToString())
                                                                                );
                                }
                                else
                                {
                                    continue;
                                }
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            dictionary_uniqSNILS_OTMN_SZV_M[
                                     ConvertRegNom(reader[0].ToString()) +
                                     Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn +
                                     reader[1].ToString() +
                                     Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp +
                                     reader[3].ToString()
                                                                ] =
                                                             new DataFromPersoDB(
                                                                ConvertRegNom(reader[0].ToString()),
                                                                reader[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader[0].ToString())].insurer_kpp,
                                                                reader[2].ToString(),
                                                                reader[3].ToString(),
                                                                Convert.ToDateTime(reader[4].ToString()),
                                                                Convert.ToDateTime(reader[5].ToString())
                                                                                );
                        }

                        //i_OTMN++;
                    }
                    reader.Close();

                    Console.WriteLine("Количество выбранных строк из БД Perso (ОТМН формы СЗВ-М): {0} ", dictionary_uniqSNILS_OTMN_SZV_M.Count());
                    //Console.WriteLine();




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



        public static void Compare_SZV_M_ISX_and_OTMN()
        {
            try
            {
                //Формируем реестр уникальных СНИЛС СЗВ-М с учетом отмененных форм
                foreach (var item_uniqSNILS_ISXD_SZV_M in dictionary_uniqSNILS_ISXD_SZV_M)
                {
                    //регНом+СНИЛС есть в словаре
                    DataFromPersoDB tmpData = new DataFromPersoDB();
                    if (dictionary_uniqSNILS_OTMN_SZV_M.TryGetValue(item_uniqSNILS_ISXD_SZV_M.Key, out tmpData))
                    {
                        //сверяем даты импорта в БД (больше)
                        if (item_uniqSNILS_ISXD_SZV_M.Value.dateINS > tmpData.dateINS)
                        {
                            //есть в словаре
                            DataFromPersoDB tmpData_no_OTMN = new DataFromPersoDB();
                            if (Program.uniqSNILS_ISXD_SZV_M_no_OTMN.TryGetValue(item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp, out tmpData_no_OTMN))
                            {
                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                            new DataFromPersoDB(
                                                                item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                                AddOtchMonth(
                                                                    Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp].otchMonth,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                    )
                                                                                );
                            }
                            else
                            {
                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                            new DataFromPersoDB(
                                                                item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                                AddOtchMonth(
                                                                    "",
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                    )
                                                                                );
                            }
                                                                                                                                                              

                            //KEY: INN + SNILS + KPP
                            //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp + item_uniqSNILS_ISXD_SZV_M.Value.otchMonth]
                            //    = item_uniqSNILS_ISXD_SZV_M.Value;

                            //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ
                            Program.hashSet_SZVM_UniqSNILS.Add(item_uniqSNILS_ISXD_SZV_M.Value.inn + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.strnum + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.kpp + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                              );

                        }
                        //сверяем даты импорта в БД (равны)
                        else if (item_uniqSNILS_ISXD_SZV_M.Value.dateINS == tmpData.dateINS)
                        {
                            //тогда сверяем время импорта в БД (больше)
                            if (item_uniqSNILS_ISXD_SZV_M.Value.timeINS > tmpData.timeINS)
                            {
                                //есть в словаре
                                DataFromPersoDB tmpData_no_OTMN = new DataFromPersoDB();
                                if (Program.uniqSNILS_ISXD_SZV_M_no_OTMN.TryGetValue(item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp, out tmpData_no_OTMN))
                                {
                                    Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                                new DataFromPersoDB(
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                                    AddOtchMonth(
                                                                        Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp].otchMonth,
                                                                        item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                        )
                                                                                    );
                                }
                                else
                                {
                                    Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                                new DataFromPersoDB(
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                                    item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                                    AddOtchMonth(
                                                                        "",
                                                                        item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                        )
                                                                                    );
                                }                               


                                //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp + item_uniqSNILS_ISXD_SZV_M.Value.otchMonth]
                                //= item_uniqSNILS_ISXD_SZV_M.Value;

                                //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ
                                Program.hashSet_SZVM_UniqSNILS.Add(item_uniqSNILS_ISXD_SZV_M.Value.inn + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.strnum + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.kpp + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                              );
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }
                    else
                    {
                        //нет в словаре
                        DataFromPersoDB tmpData_no_OTMN = new DataFromPersoDB();
                        if (Program.uniqSNILS_ISXD_SZV_M_no_OTMN.TryGetValue(item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp, out tmpData_no_OTMN))
                        {
                            Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                        new DataFromPersoDB(
                                                            item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                            AddOtchMonth(
                                                                Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp].otchMonth,
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                )
                                                                            );
                        }
                        else
                        {
                            Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp] =
                                                        new DataFromPersoDB(
                                                            item_uniqSNILS_ISXD_SZV_M.Value.regNum,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.strnum,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.inn,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.kpp,
                                                            item_uniqSNILS_ISXD_SZV_M.Value.otchYear,

                                                            AddOtchMonth(
                                                                "",
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                                )
                                                                            );
                        }
                        //Program.uniqSNILS_ISXD_SZV_M_no_OTMN[item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp + item_uniqSNILS_ISXD_SZV_M.Value.otchMonth]
                        //        = item_uniqSNILS_ISXD_SZV_M.Value;

                        //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-СТАЖ
                        Program.hashSet_SZVM_UniqSNILS.Add(item_uniqSNILS_ISXD_SZV_M.Value.inn + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.strnum + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.kpp + "," +
                                                                item_uniqSNILS_ISXD_SZV_M.Value.otchMonth
                                                              );
                    }
                }


                //Console.Write("");

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
            SortedSet<int> tmpStr = new SortedSet<int>();

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
        private static string SelectRaion(string regNum)
        {
            try
            {
                if (regNum.Count() == 11)
                {
                    return "042-0" + regNum[3] + regNum[4];
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

        //------------------------------------------------------------------------------------------
        private static string ConvertRegNom(string regNom)
        {
            try
            {
                char[] regNomOld = regNom.ToCharArray();
                string regNomConvert = "0" + regNomOld[0].ToString() + regNomOld[1].ToString() + "-" + regNomOld[2].ToString() + regNomOld[3] + regNomOld[4] + "-" + regNomOld[5] + regNomOld[6] + regNomOld[7] + regNomOld[8] + regNomOld[9] + regNomOld[10];


                return regNomConvert;
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

        ////------------------------------------------------------------------------------------------        
        ////Формируем результирующий файл на основании данных из БД
        //public static void CreateExportFileSNILS(string zagolovok, Dictionary<string, DataFromPersoDB> uniqSNILS_ISXD_SZV_M_no_OTMN, string nameFile)
        //{
        //    try
        //    {
        //        //Добавляем в файл данные                
        //        using (StreamWriter writer = new StreamWriter(nameFile, true, Encoding.GetEncoding(1251)))
        //        {
        //            writer.WriteLine(zagolovok);

        //            int i = 0;

        //            foreach (var item in uniqSNILS_ISXD_SZV_M_no_OTMN)
        //            {
        //                i++;
        //                writer.Write(i + ";");
        //                writer.WriteLine(item.Value.ToString() + ";");
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        IOoperations.WriteLogError(ex.ToString());

        //        Console.WriteLine();
        //        Console.ForegroundColor = ConsoleColor.Red;
        //        Console.WriteLine(ex.Message);
        //        Console.ForegroundColor = ConsoleColor.Gray;
        //    }
        //}


        ////------------------------------------------------------------------------------------------        
        ////Формируем результирующий файл на основании данных из БД
        //public static void CreateExportFile(string zagolovok, Dictionary<string, int> dictionary_svodDataFromPersoDB_UniqSNILS_SZVM, string nameFile)
        //{
        //    try
        //    {
        //        //Добавляем в файл данные                
        //        using (StreamWriter writer = new StreamWriter(nameFile, true, Encoding.GetEncoding(1251)))
        //        {
        //            writer.WriteLine(zagolovok);

        //            foreach (var item in dictionary_svodDataFromPersoDB_UniqSNILS_SZVM)
        //            {
        //                writer.WriteLine(item.Key.ToString() + ";" + item.Value.ToString() + ";");
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        IOoperations.WriteLogError(ex.ToString());

        //        Console.WriteLine();
        //        Console.ForegroundColor = ConsoleColor.Red;
        //        Console.WriteLine(ex.Message);
        //        Console.ForegroundColor = ConsoleColor.Gray;
        //    }
        //}

    }
}


////наполняем словарь dictionary_uniqSNILS_ISXD_SZV_M последним по Дате (Времени) регНом+СНИЛС
//foreach (DataFromPersoDB_SZVM_Compare_v2 itemDataPerso in listReestrSZV_M_ISXD)
//{
//    //регНом+СНИЛС есть в словаре
//    DataFromPersoDB_SZVM_Compare_v2 tmpData = new DataFromPersoDB_SZVM_Compare_v2();
//    if (dictionary_uniqSNILS_ISXD_SZV_M.TryGetValue(itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth, out tmpData))
//    {
//        //сверяем даты импорта в БД (больше)
//        if (Convert.ToDateTime(itemDataPerso.dateINS) > Convert.ToDateTime(tmpData.dateINS))
//        {
//            dictionary_uniqSNILS_ISXD_SZV_M[itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth] = itemDataPerso;
//        }
//        //сверяем даты импорта в БД (равны)
//        if (Convert.ToDateTime(itemDataPerso.dateINS) == Convert.ToDateTime(tmpData.dateINS))
//        {
//            //тогда сверяем время импорта в БД (больше)
//            if (Convert.ToDateTime(itemDataPerso.timeINS) > Convert.ToDateTime(tmpData.timeINS))
//            {
//                dictionary_uniqSNILS_ISXD_SZV_M[itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth] = itemDataPerso;
//            }
//            else
//            {
//                continue;
//            }
//        }
//        else
//        {
//            continue;
//        }
//    }
//    else
//    {
//        dictionary_uniqSNILS_ISXD_SZV_M.Add(itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth, itemDataPerso);
//    }
//}



////наполняем словарь dictionary_uniqSNILS_OTMN_SZV_M последним по Дате (Времени) регНом+СНИЛС
//foreach (DataFromPersoDB_SZVM_Compare_v2 itemDataPerso in listReestrSZV_M_OTMN)
//{
//    //регНом+СНИЛС есть в словаре
//    DataFromPersoDB_SZVM_Compare_v2 tmpData = new DataFromPersoDB_SZVM_Compare_v2();
//    if (dictionary_uniqSNILS_OTMN_SZV_M.TryGetValue(itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth, out tmpData))
//    {
//        //сверяем даты импорта в БД (больше)
//        if (Convert.ToDateTime(itemDataPerso.dateINS) > Convert.ToDateTime(tmpData.dateINS))
//        {
//            dictionary_uniqSNILS_OTMN_SZV_M[itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth] = itemDataPerso;
//        }
//        //сверяем даты импорта в БД (равны)
//        if (Convert.ToDateTime(itemDataPerso.dateINS) == Convert.ToDateTime(tmpData.dateINS))
//        {
//            //тогда сверяем время импорта в БД (больше)
//            if (Convert.ToDateTime(itemDataPerso.timeINS) > Convert.ToDateTime(tmpData.timeINS))
//            {
//                dictionary_uniqSNILS_OTMN_SZV_M[itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth] = itemDataPerso;
//            }
//            else
//            {
//                continue;
//            }
//        }
//        else
//        {
//            continue;
//        }
//    }
//    else
//    {
//        dictionary_uniqSNILS_OTMN_SZV_M.Add(itemDataPerso.regNum + itemDataPerso.strnum + itemDataPerso.otchMonth, itemDataPerso);
//    }
//}