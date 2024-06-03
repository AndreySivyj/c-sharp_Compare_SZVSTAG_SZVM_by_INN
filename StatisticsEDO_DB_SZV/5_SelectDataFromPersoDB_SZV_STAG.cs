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
    public class DataFromPersoDB_SZVSTAG
    {
        public string regNum;
        public string strnum;
        public string inn;
        public string kpp;
        public string otchYear;

        public int monthPeriodS;
        public int monthPeriodPo;

        //public string otchMonth;

        public DateTime dateINS;
        public DateTime timeINS;
        public string tip;


        public DataFromPersoDB_SZVSTAG(string regNum = "", string strnum = "",
                                string inn = "", string kpp = "", string otchYear = "",
                                int monthPeriodS = 0, int monthPeriodPo = 0,
                                DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime),
                                string tip = "")
        {
            this.regNum = regNum;
            this.strnum = strnum;
            this.inn = inn;
            this.kpp = kpp;
            this.otchYear = otchYear;

            this.monthPeriodS = monthPeriodS;
            this.monthPeriodPo = monthPeriodPo;

            this.dateINS = dateINS;
            this.timeINS = timeINS;

            this.tip = tip;
        }

        public override string ToString()
        {
            return regNum + ";" + strnum + ";" + inn + ";" + kpp + ";" + otchYear + ";" + monthPeriodS + ";" + monthPeriodPo + ";" + dateINS + ";" + timeINS + ";" + tip + ";";
        }
    }



    class SelectDataFromPersoDB_SZV_STAG
    {
        private static List<DataFromPersoDB_SZVSTAG> list_uniqSNILS_ISXD_SZV_STAG = new List<DataFromPersoDB_SZVSTAG>();       //Коллекция данных

        //private static Dictionary<string, DataFromPersoDB> dictionary_uniqSNILS_ISXD_SZV_STAG = new Dictionary<string, DataFromPersoDB>();       //Коллекция данных
        private static Dictionary<string, DataFromPersoDB> dictionary_uniqSNILS_ISXD_SZV_STAG_Actual = new Dictionary<string, DataFromPersoDB>();       //Коллекция данных

        private static Dictionary<string, DataFromPersoDB> dictionary_uniqSNILS_OTMN_SZV_STAG = new Dictionary<string, DataFromPersoDB>();       //Коллекция данных        


        //------------------------------------------------------------------------------------------        
        //Выбираем данные из БД Perso

        async public static void SelectDataFromPersoDB_ISXD(string query)
        //async public static void SelectDataFromPersoDB(string query, string regNumItem, Dictionary<string, int> dictionary_svodDataFromPersoDB_UniqSNILS_SZVSTAG)
        {
            //Подключаемся к БД и выполняем запрос
            using (DB2Connection connection = new DB2Connection("Server=1.1.1.1:50000;Database=PERSDB;UID=regusr;PWD=password;"))
            {
                try
                {
                    //открываем соединение
                    await connection.OpenAsync();

                    DB2Command command = connection.CreateCommand();
                    command.CommandText = query;

                    //Устанавливаем значение таймаута
                    command.CommandTimeout = 570;

                    DbDataReader reader_ISXD = await command.ExecuteReaderAsync();




                    //int i = 0;

                    while (await reader_ISXD.ReadAsync())
                    {
                        list_uniqSNILS_ISXD_SZV_STAG.Add(
                                                    new DataFromPersoDB_SZVSTAG(
                                                        //ConvertRegNom(reader_ISXD[0].ToString()),
                                                        reader_ISXD[0].ToString(),
                                                        reader_ISXD[1].ToString(),
                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_inn,
                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_ISXD[0].ToString())].insurer_kpp,
                                                        Program.otchYear,
                                                        Convert.ToDateTime(reader_ISXD[2].ToString()).Month,
                                                        Convert.ToDateTime(reader_ISXD[3].ToString()).Month,
                                                        Convert.ToDateTime(reader_ISXD[4].ToString()),
                                                        Convert.ToDateTime(reader_ISXD[5].ToString()),
                                                        reader_ISXD[6].ToString()
                                                                                )
                                                        );
                       


                    }
                    reader_ISXD.Close();


                    //------------------------------------------------------------------------------------------        
                    //Выбираем последнюю принятую\непроверенную запись по снилс (учитываем корректировки)
                    Actual_SelectDataFromPersoDB_ISXD();



                    //Console.WriteLine("Количество выбранных строк из БД Perso (ИСХД формы СЗВ-СТАЖ): {0} ", dictionary_uniqSNILS_ISXD_SZV_STAG.Count());

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







        private static void Actual_SelectDataFromPersoDB_ISXD()
        {

            try
            {

                foreach (var item_list_uniqSNILS_ISXD_SZV_STAG in list_uniqSNILS_ISXD_SZV_STAG)
                {
                    //v1------------------------------------------------------------------------------------------
                    //если tip == "СЗВ-СТАЖ"
                    if (item_list_uniqSNILS_ISXD_SZV_STAG.tip == "СЗВ-СТАЖ")
                    {
                        //KEY:   регНом + ИНН + СНИЛС + КПП   есть в словаре есть в словаре
                        DataFromPersoDB tmpData = new DataFromPersoDB();
                        if (dictionary_uniqSNILS_ISXD_SZV_STAG_Actual.TryGetValue(
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                     out tmpData))
                        {
                            //сверяем даты импорта в БД (больше)
                            if (item_list_uniqSNILS_ISXD_SZV_STAG.dateINS > tmpData.dateINS)
                            {
                                dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  AddOtchMonth(
                                                                                                                      dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                                                                                                              ].otchMonth,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                            }
                            //сверяем даты импорта в БД (равны)
                            else if (item_list_uniqSNILS_ISXD_SZV_STAG.dateINS == Convert.ToDateTime(tmpData.dateINS))
                            {
                                //тогда сверяем время импорта в БД (больше)
                                if (item_list_uniqSNILS_ISXD_SZV_STAG.timeINS >= tmpData.timeINS)
                                {
                                    dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  AddOtchMonth(
                                                                                                                      dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                                                                                                              ].otchMonth,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
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
                            dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  AddOtchMonth(
                                                                                                                      "",
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS, item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                        }



                    }
                    //------------------------------------------------------------------------------------------
                    else   //если tip == "СЗВ-КОРР"
                    {
                        //KEY:   регНом + ИНН + СНИЛС + КПП   есть в словаре есть в словаре
                        DataFromPersoDB tmpData = new DataFromPersoDB();
                        if (dictionary_uniqSNILS_ISXD_SZV_STAG_Actual.TryGetValue(
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                                    item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                     out tmpData))
                        {
                            //сверяем даты импорта в БД (больше)
                            if (item_list_uniqSNILS_ISXD_SZV_STAG.dateINS > tmpData.dateINS)
                            {
                                dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  ReplaceOtchMonth(
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                            }
                            //сверяем даты импорта в БД (равны)
                            else if (item_list_uniqSNILS_ISXD_SZV_STAG.dateINS == Convert.ToDateTime(tmpData.dateINS))
                            {
                                //тогда сверяем время импорта в БД (больше)
                                if (item_list_uniqSNILS_ISXD_SZV_STAG.timeINS > tmpData.timeINS)
                                {
                                    dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  ReplaceOtchMonth(
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                                }
                                else if (item_list_uniqSNILS_ISXD_SZV_STAG.timeINS == tmpData.timeINS)
                                {
                                    //Console.WriteLine(item_list_uniqSNILS_ISXD_SZV_STAG.regNum + "   "
                                    //    + item_list_uniqSNILS_ISXD_SZV_STAG.strnum + "   "
                                    //    + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS + "   "
                                    //    + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo + "   "
                                    //    + item_list_uniqSNILS_ISXD_SZV_STAG.dateINS + "   "
                                    //    + item_list_uniqSNILS_ISXD_SZV_STAG.timeINS);

                                    dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  AddOtchMonth(
                                                                                                                      dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                                                                                                              item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                                                                                                              ].otchMonth,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS,
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                                }
                                else
                                {
                                    Console.WriteLine(item_list_uniqSNILS_ISXD_SZV_STAG.regNum + "   "
                                        + item_list_uniqSNILS_ISXD_SZV_STAG.strnum + "   "
                                        + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS + "   "
                                        + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo + "   "
                                        + item_list_uniqSNILS_ISXD_SZV_STAG.dateINS + "   "
                                        + item_list_uniqSNILS_ISXD_SZV_STAG.timeINS);

                                    continue;
                                }
                            }
                            else
                            {
                                //Console.WriteLine(item_list_uniqSNILS_ISXD_SZV_STAG.regNum + "   "
                                //        + item_list_uniqSNILS_ISXD_SZV_STAG.strnum + "   "
                                //        + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS + "   "
                                //        + item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo + "   "
                                //        + item_list_uniqSNILS_ISXD_SZV_STAG.dateINS + "   "
                                //        + item_list_uniqSNILS_ISXD_SZV_STAG.timeINS);

                                continue;
                            }

                        }
                        else
                        {
                            dictionary_uniqSNILS_ISXD_SZV_STAG_Actual[
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.regNum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.inn +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.strnum +
                                                                          item_list_uniqSNILS_ISXD_SZV_STAG.kpp
                                                                          ]
                                                                          =
                                                                          new DataFromPersoDB(
                                                                                                                  ConvertRegNom(item_list_uniqSNILS_ISXD_SZV_STAG.regNum),
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.strnum,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.inn,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.kpp,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.otchYear,

                                                                                                                  ReplaceOtchMonth(                                                                                                                      
                                                                                                                      item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodS, item_list_uniqSNILS_ISXD_SZV_STAG.monthPeriodPo
                                                                                                                      ),

                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.dateINS,
                                                                                                                  item_list_uniqSNILS_ISXD_SZV_STAG.timeINS
                                                                                                                                  );
                        }

                    }

                }



                Console.WriteLine("Количество выбранных строк из БД Perso (ИСХ формы СЗВ-СТАЖ с учетом СЗВ-КОРР): {0} ", dictionary_uniqSNILS_ISXD_SZV_STAG_Actual.Count());





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

        //
        private static string AddOtchMonth(string otchMonth, int monthPeriodS, int monthPeriodPo)
        {
            //Создаем массив уникальных месяцев из dictionary_uniqSNILS_ISXD_SZV_STAG.Value.otchMonth
            SortedSet<int> tmpStr = new SortedSet<int>();

            char[] separator = { ',' };    //список разделителей в строке
            string[] massiveStr = otchMonth.Split(separator);     //создаем массив из строк между разделителями

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



            //Создаем строку из месяцев, входящих в стажевый период            

            if (monthPeriodS < monthPeriodPo)
            {
                while (monthPeriodS != monthPeriodPo)
                {
                    tmpStr.Add(monthPeriodS);

                    monthPeriodS++;
                }
                tmpStr.Add(monthPeriodPo);
            }
            else
            {
                //!!! стажевый период "С" больше "ПО"
                tmpStr.Add(monthPeriodS);
                tmpStr.Add(monthPeriodPo);
            }



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

        private static string ReplaceOtchMonth(int monthPeriodS, int monthPeriodPo)
        {
            //Создаем массив уникальных месяцев из dictionary_uniqSNILS_ISXD_SZV_STAG.Value.otchMonth
            SortedSet<int> tmpStr = new SortedSet<int>();

            //Создаем строку из месяцев, входящих в стажевый период            

            if (monthPeriodS < monthPeriodPo)
            {
                while (monthPeriodS != monthPeriodPo)
                {
                    tmpStr.Add(monthPeriodS);

                    monthPeriodS++;
                }
                tmpStr.Add(monthPeriodPo);
            }
            else
            {
                //!!! стажевый период "С" больше "ПО"
                tmpStr.Add(monthPeriodS);
                tmpStr.Add(monthPeriodPo);
            }



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


        async public static void SelectDataFromPersoDB_OTMN(string query)
        {
            //Подключаемся к БД и выполняем запрос
            using (DB2Connection connection = new DB2Connection("Server=1.1.1.1:50000;Database=PERSDB;UID=regusr;PWD=password;"))
            {
                try
                {
                    //открываем соединение
                    await connection.OpenAsync();

                    DB2Command command = connection.CreateCommand();
                    command.CommandText = query;

                    //Устанавливаем значение таймаута
                    command.CommandTimeout = 570;

                    DbDataReader reader_OTMN = await command.ExecuteReaderAsync();




                    //int i = 0;

                    while (await reader_OTMN.ReadAsync())
                    {
                        //TODO: Придумать как выбирать отчМесяц из g.DATE_BEG, g.DATE_END
                        //                        0         1       2               3                    
                        // @"select distinct s.regnumb, w.strnum, o.DATE_INS, o.TIME_INS "



                        //public DataFromPersoDB(string raion = "", string regNum = "", string strnum = "",
                        //        string nameStrah = "", string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                        //        string kurator = "", DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))



                        //регНом+СНИЛС есть в словаре
                        DataFromPersoDB tmpData = new DataFromPersoDB();
                        if (dictionary_uniqSNILS_OTMN_SZV_STAG.TryGetValue(
                                                                        reader_OTMN[0].ToString() +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn +
                                                                        reader_OTMN[1].ToString() +
                                                                        Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp,
                                                                                                                                                                 out tmpData))
                        {
                            //сверяем даты импорта в БД (больше)
                            if (Convert.ToDateTime(reader_OTMN[2].ToString()) > tmpData.dateINS)
                            {
                                //KEY: INN + SNILS + KPP
                                dictionary_uniqSNILS_OTMN_SZV_STAG[
                                    reader_OTMN[0].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn +
                                    reader_OTMN[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_OTMN[0].ToString()),
                                                                reader_OTMN[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp,
                                                                Program.otchYear,
                                                                "",
                                                                Convert.ToDateTime(reader_OTMN[2].ToString()),
                                                                Convert.ToDateTime(reader_OTMN[3].ToString())
                                                                                );
                            }
                            //сверяем даты импорта в БД (равны)
                            else if (Convert.ToDateTime(reader_OTMN[2].ToString()) == Convert.ToDateTime(tmpData.dateINS))
                            {
                                //тогда сверяем время импорта в БД (больше)
                                if (Convert.ToDateTime(reader_OTMN[3].ToString()) >= tmpData.timeINS)
                                {
                                    dictionary_uniqSNILS_OTMN_SZV_STAG[
                                    reader_OTMN[0].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn +
                                    reader_OTMN[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_OTMN[0].ToString()),
                                                                reader_OTMN[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp,
                                                                Program.otchYear,
                                                                "",
                                                                Convert.ToDateTime(reader_OTMN[2].ToString()),
                                                                Convert.ToDateTime(reader_OTMN[3].ToString())
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
                            dictionary_uniqSNILS_OTMN_SZV_STAG[
                                    reader_OTMN[0].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn +
                                    reader_OTMN[1].ToString() +
                                    Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp
                                                               ] =
                                                            new DataFromPersoDB(
                                                                ConvertRegNom(reader_OTMN[0].ToString()),
                                                                reader_OTMN[1].ToString(),
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_inn,
                                                                Program.dictionary_dataFromPKASVDB[ConvertRegNom(reader_OTMN[0].ToString())].insurer_kpp,
                                                                Program.otchYear,
                                                                "",
                                                                Convert.ToDateTime(reader_OTMN[2].ToString()),
                                                                Convert.ToDateTime(reader_OTMN[3].ToString())
                                                                                );
                        }

                        //i++;
                    }
                    reader_OTMN.Close();

                    Console.WriteLine("Количество выбранных строк из БД Perso (ОТМН формы СЗВ-СТАЖ): {0} ", dictionary_uniqSNILS_OTMN_SZV_STAG.Count());





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

        public static void Compare_SZV_STAG_ISX_and_OTMN()
        {
            try
            {
                //Формируем реестр уникальных СНИЛС СЗВ-М с учетом отмененных форм
                foreach (var item_uniqSNILS_ISXD_SZV in dictionary_uniqSNILS_ISXD_SZV_STAG_Actual)
                {
                    //регНом+СНИЛС есть в словаре
                    DataFromPersoDB tmpData = new DataFromPersoDB();
                    if (dictionary_uniqSNILS_OTMN_SZV_STAG.TryGetValue(item_uniqSNILS_ISXD_SZV.Key, out tmpData))
                    {
                        //сверяем даты импорта в БД (больше)
                        if (item_uniqSNILS_ISXD_SZV.Value.dateINS > tmpData.dateINS)
                        {
                            //KEY: INN + SNILS + KPP
                            Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[item_uniqSNILS_ISXD_SZV.Value.inn + item_uniqSNILS_ISXD_SZV.Value.strnum + item_uniqSNILS_ISXD_SZV.Value.kpp]
                                = item_uniqSNILS_ISXD_SZV.Value;

                            //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-М
                            AddDataInHashSet(item_uniqSNILS_ISXD_SZV.Value);
                            //Program.hashSet_SZV_STAG_UniqSNILS.Add(item_uniqSNILS_ISXD_SZV_M.Value.inn + item_uniqSNILS_ISXD_SZV_M.Value.strnum + item_uniqSNILS_ISXD_SZV_M.Value.kpp + );
                        }
                        //сверяем даты импорта в БД (равны)
                        else if (item_uniqSNILS_ISXD_SZV.Value.dateINS == tmpData.dateINS)
                        {
                            //тогда сверяем время импорта в БД (больше)
                            if (item_uniqSNILS_ISXD_SZV.Value.timeINS > tmpData.timeINS)
                            {
                                Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[item_uniqSNILS_ISXD_SZV.Value.inn + item_uniqSNILS_ISXD_SZV.Value.strnum + item_uniqSNILS_ISXD_SZV.Value.kpp]
                                = item_uniqSNILS_ISXD_SZV.Value;

                                //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-М
                                AddDataInHashSet(item_uniqSNILS_ISXD_SZV.Value);
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
                        Program.uniqSNILS_ISXD_SZV_STAG_no_OTMN[item_uniqSNILS_ISXD_SZV.Value.inn + item_uniqSNILS_ISXD_SZV.Value.strnum + item_uniqSNILS_ISXD_SZV.Value.kpp]
                                = item_uniqSNILS_ISXD_SZV.Value;

                        //Формируем массив уникальных KEY (INN + SNILS + KPP + otchMonth) для сверки с СЗВ-М
                        AddDataInHashSet(item_uniqSNILS_ISXD_SZV.Value);
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

        private static void AddDataInHashSet(DataFromPersoDB item_uniqSNILS_ISXD_SZV_Value)
        {
            try
            {
                char[] separator = { ',' };    //список разделителей в строке

                string[] massiveStr = item_uniqSNILS_ISXD_SZV_Value.otchMonth.Split(separator);     //создаем массив из строк между разделителями

                foreach (var item in massiveStr)
                {
                    if (item != "")
                    {
                        //TODO: Внимание! проверить, что за нулевой месяц подтягивался
                        if (item != "0")
                        {
                            Program.hashSet_SZV_STAG_UniqSNILS.Add(item_uniqSNILS_ISXD_SZV_Value.inn + "," +
                                                                item_uniqSNILS_ISXD_SZV_Value.strnum + "," +
                                                                item_uniqSNILS_ISXD_SZV_Value.kpp + "," +
                                                                item
                                                              );
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                IOoperations.WriteLogError(ex.ToString());
            }
        }




        ////------------------------------------------------------------------------------------------        
        ////Формируем результирующий файл на основании данных из БД
        //public static void CreateExportFile(string zagolovok, List<DataFromPersoDB_ISXDform> listData, string nameFile)
        //{
        //    try
        //    {
        //        //Добавляем в файл данные                
        //        using (StreamWriter writer = new StreamWriter(nameFile, true, Encoding.GetEncoding(1251)))
        //        {
        //            writer.WriteLine(zagolovok);

        //            int i = 0;

        //            foreach (var item in listData)
        //            {
        //                i++;
        //                writer.Write(i + ";");
        //                writer.WriteLine(item.ToString());
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

        //------------------------------------------------------------------------------------------
        private static string ConvertDataFromDB(string dataTime)
        {
            try
            {
                if (dataTime != "")
                {
                    DateTime date = Convert.ToDateTime(dataTime);
                    return date.ToShortDateString();
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
        private static string ConvertTimeFromDB(string dataTime)
        {
            try
            {
                if (dataTime != "")
                {
                    DateTime date = Convert.ToDateTime(dataTime);
                    return date.ToShortTimeString();
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
        private static string ConvertRegNom(string regNum)
        {
            try
            {
                if (regNum.Count() == 11)
                {
                    char[] regNomOld = regNum.ToCharArray();
                    string regNomConvert = "0" + regNomOld[0].ToString() + regNomOld[1].ToString() + "-" + regNomOld[2].ToString() + regNomOld[3] + regNomOld[4] + "-" + regNomOld[5] + regNomOld[6] + regNomOld[7] + regNomOld[8] + regNomOld[9] + regNomOld[10];

                    return regNomConvert;
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

    }
}
