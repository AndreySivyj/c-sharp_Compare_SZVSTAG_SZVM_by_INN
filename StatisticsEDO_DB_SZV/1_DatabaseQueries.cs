using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Compare_SZVSTAG_SZVM
{
    static class DatabaseQueries
    {

        //------------------------------------------------------------------------------------------
        public static string CreatequeryRKASV(SortedSet<string> sortedSet_INN)
        {
            //string queryRKASV = @"select a.insurer_reg_num, a.insurer_inn, a.insurer_kpp, a.insurer_short_name, a.insurer_last_name, a.insurer_first_name, a.insurer_middle_name " +
            //                    @"from(select * FROM asv_insurer) a " +
            //                    @"where  a.insurer_inn = " + inn +
            //                    @" order by a.insurer_reg_num";
            
            //две части текста запроса к РК АСВ             
            string part1 =
                @"select a.insurer_reg_num, a.insurer_inn, a.insurer_kpp, a.insurer_short_name, a.insurer_last_name, a.insurer_first_name, a.insurer_middle_name " +
                @"from(select * FROM asv_insurer) a " +
                @"where  a.insurer_inn in (";

            string part2 = @") order by a.insurer_reg_num";

            //Текст запроса
            string queryRKASV = part1;

            int tmpCount = sortedSet_INN.Count;

            foreach (var item in sortedSet_INN)
            {
                --tmpCount;
                if (tmpCount != 0)
                {
                    queryRKASV = queryRKASV + item + ", ";
                }
                else
                {
                    queryRKASV = queryRKASV + item;
                }
            }

            queryRKASV = queryRKASV + part2;
            
            return queryRKASV;
        }

        //------------------------------------------------------------------------------------------
        public static string CreateQueryPersoSZVSTAG_OTMN()
        {
            //две части текста запроса     
            string part1 = @"select distinct s.regnumb, w.strnum, o.DATE_INS, o.TIME_INS "
                           + @"from pers.odv_1 o, pers.works_odv w, pers.stag_odv g, pers.strah s "
                           + @"where w.cod_szv = o.cod_zap "
                           + @"and o.cod_org = s.cod_zap "
                           + @"and w.korgod=" + Program.otchYear
                           + @" and o.tip in ('СЗВ-СТАЖ', 'СЗВ-КОРР') "
                           + @"and o.tip_sveden in ('ОТМЕНЯЮЩАЯ') "
                           + @"and s.regnumb in (";

            string part2 = @") and w.STVIO in (0,4,1) "
                           + @"and o.STATUS_REC <> '' "
                           + @"order by s.regnumb, w.strnum";



            //Текст запроса
            string queryPersoReestrOTMN = part1;

            int tmpCount = Program.sortedSet_RegNom_For_Select.Count;

            foreach (var item in Program.sortedSet_RegNom_For_Select)
            {
                --tmpCount;
                if (tmpCount != 0)
                {
                    queryPersoReestrOTMN = queryPersoReestrOTMN + item + ", ";
                }
                else
                {
                    queryPersoReestrOTMN = queryPersoReestrOTMN + item;
                }
            }

            queryPersoReestrOTMN = queryPersoReestrOTMN + part2;

            return queryPersoReestrOTMN;

        }

        //------------------------------------------------------------------------------------------
        //СЗВ-СТАЖ с периодами стажа по СНИЛС, все принятые или не проверенные с датой и временем ввода
        public static string CreateQueryPersoSZVSTAG_ISXD()
        {
            string p_godPersoSTAG = Program.otchYear;

            string p_godTMP = Program.otchYear;

            int yearNow = DateTime.Now.Year;

            while (yearNow != Convert.ToInt32(p_godTMP))
            {
                p_godTMP = (Convert.ToInt32(p_godTMP) + 1).ToString();

                p_godPersoSTAG = p_godPersoSTAG + "," + p_godTMP;
            }

            //две части текста запроса
            string part1 = @"select distinct s.regnumb, w.strnum, g.DATE_BEG, g.DATE_END, o.DATE_INS, o.TIME_INS, o.tip " + 
                @"from pers.odv_1 o, pers.works_odv w, pers.stag_odv g, pers.strah s " +
                @"where " +
                @"w.cod_szv=o.cod_zap " +
                @"and g.cod_works=w.cod_zap " +
                @"and o.cod_org=s.cod_zap " +
                @"and o.god in (" + p_godPersoSTAG + ") " +                
                @"and o.tip in ('СЗВ-СТАЖ','СЗВ-КОРР') " +                
                @"and o.tip_sveden in ('ИСХОДНАЯ','ДОПОЛНЯЮЩАЯ','КОРРЕКТИРУЮЩАЯ') " +
                @"and year(g.date_beg)=" + Program.otchYear + " " +
                @"and year(g.date_end)=" + Program.otchYear + " " +
                @"and s.regnumb in (";


            string part2 = @") and w.stvio in (0,4,1) " + //'Не проверен', 'Проверен, принят'
                @"and o.STATUS_REC <> '' " +
                @"order by s.regnumb, w.strnum";

            //Текст запроса
            string queryPersoSTAG_ISX = part1;

            int tmpCount = Program.sortedSet_RegNom_For_Select.Count;

            foreach (var item in Program.sortedSet_RegNom_For_Select)
            {
                --tmpCount;
                if (tmpCount != 0)
                {
                    queryPersoSTAG_ISX = queryPersoSTAG_ISX + item + ", ";
                }
                else
                {
                    queryPersoSTAG_ISX = queryPersoSTAG_ISX + item;
                }
            }

            queryPersoSTAG_ISX = queryPersoSTAG_ISX + part2;

            return queryPersoSTAG_ISX;

        }

        //------------------------------------------------------------------------------------------
        //Запрос по СЗВ-М, все принятые или не проверенные с датой и временем ввода        
        public static string CreateQueryPersoSZVM_ISX()
        {
            //две части текста запроса
            string part1 = @"select distinct p.regnumb, w.strnum, w.god, w.period, s.DATE_INS, s.TIME_INS " +
            @"from pers.WORKS_M w, pers.SZV_M s, pers.STRAH p " +
            @"where w.cod_szv = s.cod_zap " +
            @"and s.cod_org = p.cod_zap " +
            @"and s.GOD=" + Program.otchYear + " " +
            @"and w.tip_form in ('ИСХОДНАЯ','ДОПОЛНЯЮЩАЯ') " +
            @"and p.regnumb in (";


            string part2 = @") and w.stvio in (0, 1, 4) " + //'Не проверен', 'Проверен, принят'
               @"and s.STATUS_REC <> '' " +
               @"order by p.regnumb, w.strnum, w.god, w.period";


            //Текст запроса
            string queryPersoSZVM_ISX = part1;

            int tmpCount = Program.sortedSet_RegNom_For_Select.Count;

            foreach (var item in Program.sortedSet_RegNom_For_Select)
            {
                --tmpCount;
                if (tmpCount != 0)
                {
                    queryPersoSZVM_ISX = queryPersoSZVM_ISX + item + ", ";
                }
                else
                {
                    queryPersoSZVM_ISX = queryPersoSZVM_ISX + item;
                }
            }

            queryPersoSZVM_ISX = queryPersoSZVM_ISX + part2;

            return queryPersoSZVM_ISX;
        }

        //------------------------------------------------------------------------------------------
        //Запрос по СЗВ-М, все принятые или не проверенные с датой и временем ввода        
        public static string CreateQueryPersoSZVM_OTMN()
        {
            //две части текста запроса
            string part1 = @"select distinct p.regnumb, w.strnum, w.god, w.period, s.DATE_INS, s.TIME_INS " +
            @"from pers.WORKS_M w, pers.SZV_M s, pers.STRAH p " +
            @"where w.cod_szv = s.cod_zap " +
            @"and s.cod_org = p.cod_zap " +
            @"and s.GOD=" + Program.otchYear + " " +
            @"and w.tip_form in ('ОТМЕНЯЮЩАЯ') " +
            @"and p.regnumb in (";


            string part2 = @") and w.stvio in (0, 1, 4) " + //'Не проверен', 'Проверен, принят'
               @"and s.STATUS_REC <> '' " +
               @"order by p.regnumb, w.strnum, w.god, w.period";

            //Текст запроса
            string queryPersoSZVM = part1;

            int tmpCount = Program.sortedSet_RegNom_For_Select.Count;

            foreach (var item in Program.sortedSet_RegNom_For_Select)
            {
                --tmpCount;
                if (tmpCount != 0)
                {
                    queryPersoSZVM = queryPersoSZVM + item + ", ";
                }
                else
                {
                    queryPersoSZVM = queryPersoSZVM + item;
                }
            }

            queryPersoSZVM = queryPersoSZVM + part2;

            return queryPersoSZVM;
        }

        //------------------------------------------------------------------------------------------
        //Запрос по СЗВ-М, все принятые или не проверенные с датой и временем ввода        
        public static string CreateQueryPersoReestrSZVM_ISX_1RegNum(string regNum)
        {
            string query = @"select distinct p.regnumb, w.strnum, s.DATE_INS, s.TIME_INS " +
            @"from pers.WORKS_M w, pers.SZV_M s, pers.STRAH p " +
            @"where w.cod_szv = s.cod_zap " +
            @"and s.cod_org = p.cod_zap " +
            @"and s.GOD=" + Program.otchYear + " " +
            @"and w.tip_form in ('ИСХОДНАЯ','ДОПОЛНЯЮЩАЯ') " +
            @"and p.regnumb=" + regNum + " " +
            @"and w.STVIO in (0, 1, 4) " +
            @"and s.STATUS_REC <> '' " +
            @"order by p.regnumb, w.strnum";

            return query;
        }

        //------------------------------------------------------------------------------------------
        //Запрос по СЗВ-М, все принятые или не проверенные с датой и временем ввода        
        public static string CreateQueryPersoReestrSZVM_OTMN_1RegNum(string regNum)
        {
            string query = @"select distinct p.regnumb, w.strnum, s.DATE_INS, s.TIME_INS " +
            @"from pers.WORKS_M w, pers.SZV_M s, pers.STRAH p " +
            @"where w.cod_szv = s.cod_zap " +
            @"and s.cod_org = p.cod_zap " +
            @"and s.GOD=" + Program.otchYear + " " +
            @"and w.tip_form in ('ИСХОДНАЯ','ДОПОЛНЯЮЩАЯ') " +
            @"and p.regnumb=" + regNum + " " +
            @"and w.STVIO in (0, 1, 4) " +
            @"and s.STATUS_REC <> '' " +
            @"order by p.regnumb, w.strnum";

            return query;
        }

    }
}
