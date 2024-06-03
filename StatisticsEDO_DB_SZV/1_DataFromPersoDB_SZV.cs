using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Compare_SZVSTAG_SZVM
{
    public class DataFromPersoDB
    {
        public string regNum;
        public string strnum;
        public string inn;
        public string kpp;
        public string otchYear;
        public string otchMonth;
        public DateTime dateINS;
        public DateTime timeINS;


        public DataFromPersoDB(string regNum = "", string strnum = "",
                                string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
                                DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))
        {
            this.regNum = regNum;
            this.strnum = strnum;
            this.inn = inn;
            this.kpp = kpp;
            this.otchYear = otchYear;
            this.otchMonth = otchMonth;
            this.dateINS = dateINS;
            this.timeINS = timeINS;
        }
        
        public override string ToString()
        {            
            return regNum + ";" + strnum + ";" + inn + ";" + kpp + ";" + otchYear + ";" + otchMonth + ";";
        }
    }




    //public class DataFromPersoDB
    //{
    //    public string raion;
    //    public string regNum;
    //    public string strnum;
    //    public string nameStrah;
    //    public string inn;
    //    public string kpp;
    //    public string otchYear;
    //    public string otchMonth = "";
    //    public string kurator;
    //    public DateTime dateINS;
    //    public DateTime timeINS;


    //    public DataFromPersoDB(string raion = "", string regNum = "", string strnum = "",
    //                            string nameStrah = "", string inn = "", string kpp = "", string otchYear = "", string otchMonth = "",
    //                            string kurator = "", DateTime dateINS = default(DateTime), DateTime timeINS = default(DateTime))
    //    {
    //        this.raion = raion;
    //        this.regNum = regNum;
    //        this.strnum = strnum;
    //        this.nameStrah = nameStrah;
    //        this.inn = inn;
    //        this.kpp = kpp;
    //        this.otchYear = otchYear;
    //        this.otchMonth = this.otchMonth + "," + otchMonth;
    //        this.kurator = kurator;
    //        this.dateINS = dateINS;
    //        this.timeINS = timeINS;
    //    }

    //    //TODO: реализовать преобразование this.otchMonth в сортированый список уникальных периодов для ToString()

    //    public override string ToString()
    //    {
    //        //return raion + ";" + regNum + ";" + strnum + ";" + nameStrah + ";" + inn + ";" + kpp + ";"
    //        //    + otchYear + ";" + otchMonth + ";" + kurator + ";" + dateINS + ";" + timeINS + ";";

    //        return raion + ";" + regNum + ";" + strnum + ";" + nameStrah + ";" + inn + ";" + kpp + ";"
    //            + otchYear + ";" + otchMonth + ";" + kurator + ";";

    //    }
    //}
}
