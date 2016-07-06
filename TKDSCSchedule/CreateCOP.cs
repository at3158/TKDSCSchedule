using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Data.SqlClient;


namespace TKDSCSchedule
{
    public class CreateCOP
    {
        public void CreateCOPGH()
        {
            try
            {
                #region 新版-以未出的訂單明細為單位
                SqlConnection sqlConn; new SqlConnection();
                SqlCommand sqlComm = new SqlCommand();
                StringBuilder sbSql = new StringBuilder();
                SqlDataAdapter adapter = new SqlDataAdapter();
                SqlCommandBuilder sqlCmdBuilder = new SqlCommandBuilder();
                SqlTransaction tran;
                SqlCommand cmd = new SqlCommand();
                DataSet dsCOPTD = new DataSet();
                DataSet dsCheckTotal = new DataSet();
                DataSet dsCOPTDTD004 = new DataSet();
                DataSet dsINVMF = new DataSet();
                int result;
                int TempCount = 0;
                int TempTotal = 0;
                int sort = 0;
                String pnum = null; 

                sqlConn = new SqlConnection("server=192.168.2.27;database=tokichoi_new;uid=sa;pwd=dsc");


                //找出未產生銷貨單的訂單明細，要先刪除ZCOPTD、ZCOPTDINVMF做清空
                sqlConn.Open();
                tran = sqlConn.BeginTransaction();

                sbSql.Clear();
                sbSql.Append("  DELETE ZCOPTD DELETE ZCOPTDINVMF ");
                sbSql.Append("  INSERT INTO ZCOPTD (TD001,TD002,TD003,TD004,TD0089)");
                sbSql.Append("  SELECT TD001 ,TD002 ,TD003,TD004,(SUM(TD008)-ISNULL((SELECT SUM(TH008) FROM COPTH WITH (NOLOCK) WHERE TH014=TD001 AND TH015=TD002 AND TH016=TD003 ),0)) AS TD0089");
                sbSql.Append("  FROM COPTD WITH (NOLOCK)");
                sbSql.Append("  WHERE  TD002>='201606010001'");
                sbSql.Append("  AND NOT EXISTS (SELECT TD001 FROM ZCOPTD WITH (NOLOCK) WHERE ZCOPTD.TD001=TD001 AND ZCOPTD.TD002=TD002 AND ZCOPTD.TD003=TD003)");
                sbSql.Append("  GROUP BY TD001 ,TD002,TD003,TD004");
                sbSql.Append("  HAVING SUM(TD008)<>ISNULL((SELECT SUM(TH008) FROM COPTH WITH (NOLOCK) WHERE TH014=TD001 AND TH015=TD002 AND TH016=TD003 ),0)");

                cmd.Connection = sqlConn;
                cmd.CommandTimeout = 60;
                cmd.CommandText = sbSql.ToString();
                cmd.Transaction = tran;
                result = cmd.ExecuteNonQuery();

                if (result == 0)
                {
                    tran.Rollback();    //交易取消
                }
                else
                {
                    tran.Commit();      //執行交易
                }
                sqlConn.Close();


                //找出未產生銷貨單的訂單單別、單號，產生銷貨時是以整張訂單一起產生，如果有不足批號，整張訂單就不自動產生銷貨單
                sbSql.Clear();
                sbSql.Append(" SELECT TD001,TD002 FROM ZCOPTD GROUP BY TD001,TD002 ");


                adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                sqlCmdBuilder = new SqlCommandBuilder(adapter);

                sqlConn.Open();
                dsCOPTD.Clear();
                adapter.Fill(dsCOPTD, "TEMPCOPTD");
                sqlConn.Close();

                foreach (DataRow dr in dsCOPTD.Tables["TEMPCOPTD"].Rows)
                {
                    //檢查庫存量是否足夠
                    sbSql.Clear();
                    sbSql.Append(" SELECT TD004,SUM(TD008) AS TD008  ");
                    sbSql.Append("  ,ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0) AS TOTALINVMF");
                    sbSql.Append("  ,ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) AS TOTALCOPTH");
                    sbSql.Append("  ,(ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0)-SUM(TD008)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) ) AS CHECKTOTAL");
                    sbSql.Append("  FROM COPTD WITH (NOLOCK)");
                    sbSql.AppendFormat("  WHERE TD001='{0}' AND TD002='{1}'", dr["TD001"].ToString(), dr["TD002"].ToString());
                    sbSql.Append("  GROUP BY TD004");
                    sbSql.Append("  HAVING (ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0)-SUM(TD008)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) )<0");

                    adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                    sqlCmdBuilder = new SqlCommandBuilder(adapter);

                    sqlConn.Open();
                    dsCheckTotal.Clear();
                    adapter.Fill(dsCheckTotal, "TEMPdsCheckTotal");
                    sqlConn.Close();

                        //檢查庫存數量足夠，庫存不為負，才新增COPTG、COPTH
                    if (dsCheckTotal.Tables["TEMPdsCheckTotal"].Rows.Count == 0)
                    {
                        //查出訂單明細中各品號及總數量
                        sbSql.Clear();
                        sbSql.Append("SELECT TD001,TD002,TD003,TD004,TD0089 FROM ZCOPTD ORDER BY  TD001,TD002,TD003 ");

                        adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                        sqlCmdBuilder = new SqlCommandBuilder(adapter);

                        sqlConn.Open();
                        dsCOPTDTD004.Clear();
                        adapter.Fill(dsCOPTDTD004, "TEMPdsCOPTDTD004");
                        sqlConn.Close();

                        sbSql.Clear();

                        foreach (DataRow drdsCOPTDTD004 in dsCOPTDTD004.Tables["TEMPdsCOPTDTD004"].Rows)
                        {
                            TempTotal = Convert.ToInt16(drdsCOPTDTD004["TD0089"].ToString());

                            //可用的批號明細，INMF-COPTH-ZCOPTDINVMF
                            sbSql.Clear();
                            sbSql.Append(" SELECT MF001,MF002,CONVERT(int,(SUM(MF008*MF010)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=MF001 AND TH017=MF002 ),0))-ISNULL((SELECT SUM(MF010) FROM ZCOPTDINVMF WHERE ZCOPTDINVMF.MF001=INVMF.MF001 AND ZCOPTDINVMF.MF002=INVMF.MF002 ),0))  AS TOTAL");
                            sbSql.Append(" FROM INVMF WITH (NOLOCK)");
                            sbSql.AppendFormat(" WHERE EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002) AND MF001='{0}'", drdsCOPTDTD004["TD004"].ToString());

                            sbSql.Append(" GROUP BY  MF001,MF002");
                            sbSql.Append(" HAVING CONVERT(int,(SUM(MF008*MF010)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=MF001 AND TH017=MF002 ),0))-ISNULL((SELECT SUM(MF010) FROM ZCOPTDINVMF WHERE ZCOPTDINVMF.MF001=INVMF.MF001 AND ZCOPTDINVMF.MF002=INVMF.MF002 ),0))>0 ");

                            adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                            sqlCmdBuilder = new SqlCommandBuilder(adapter);
                            dsINVMF.Clear();
                            adapter.Fill(dsINVMF, "TEMPdsINVMF");

                            sbSql.Clear();

                            foreach (DataRow drTEMPdsINVMF in dsINVMF.Tables["TEMPdsINVMF"].Rows)
                            {
                                //sort = sort + 1;
                                //pnum = sort.ToString().PadLeft(4, '0');
                                int I = Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString());
                                TempTotal = TempTotal - Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString());
                                //最後一批的批號滿足總數量
                                if (TempTotal <= 0)
                                {
                                    sbSql.Append(" INSERT INTO ZCOPTDINVMF (TD001,TD002,TD003,TD004,TD0089,MF001,MF002,MF010)");
                                    sbSql.AppendFormat(" VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}' )", drdsCOPTDTD004["TD001"].ToString(), drdsCOPTDTD004["TD002"].ToString(), drdsCOPTDTD004["TD003"].ToString(), drdsCOPTDTD004["TD004"].ToString(), drdsCOPTDTD004["TD0089"].ToString(), drTEMPdsINVMF["MF001"].ToString(), drTEMPdsINVMF["MF002"].ToString(), TempTotal+Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString()));

                                    break;
                                }
                                else
                                {
                                    sbSql.Append("INSERT INTO ZCOPTDINVMF (TD001,TD002,TD003,TD004,TD0089,MF001,MF002,MF010)");
                                    //sbSql.AppendFormat(" VALUES ({0},{1},{2},{3},{4),{5},{6),{7}) ", drdsCOPTDTD004["TD001"].ToString(), drdsCOPTDTD004["TD002"].ToString(), drdsCOPTDTD004["TD003"].ToString(), drdsCOPTDTD004["TD004"].ToString(), drdsCOPTDTD004["TD0089"].ToString(), drTEMPdsINVMF["MF001"].ToString(), drTEMPdsINVMF["MF002"].ToString(), Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString()));
                                    sbSql.AppendFormat(" VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}' )", drdsCOPTDTD004["TD001"].ToString(), drdsCOPTDTD004["TD002"].ToString(), drdsCOPTDTD004["TD003"].ToString(), drdsCOPTDTD004["TD004"].ToString(), drdsCOPTDTD004["TD0089"].ToString(), drTEMPdsINVMF["MF001"].ToString(), drTEMPdsINVMF["MF002"].ToString(), Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString()));
                                }
                            }

                            if (!string.IsNullOrEmpty(sbSql.ToString()))
                            {
                                sqlConn.Open();
                                tran = sqlConn.BeginTransaction();
                                cmd.Connection = sqlConn;
                                cmd.CommandTimeout = 60;
                                cmd.CommandText = sbSql.ToString();
                                cmd.Transaction = tran;
                                result = cmd.ExecuteNonQuery();

                                if (result == 0)
                                {
                                    tran.Rollback();    //交易取消
                                }
                                else
                                {
                                    tran.Commit();      //執行交易
                                }
                                sqlConn.Close();
                            }
                           
                        }
                    }
                }


                #endregion


                #region 舊版-以一整張訂單為單位
                //SqlConnection sqlConn; new SqlConnection();
                //SqlCommand sqlComm = new SqlCommand();
                //StringBuilder sbSql = new StringBuilder();
                //SqlDataAdapter adapter = new SqlDataAdapter();
                //SqlCommandBuilder sqlCmdBuilder = new SqlCommandBuilder();
                //DataSet dsCOPTD = new DataSet();
                //DataSet dsCheckTotal = new DataSet();
                //DataSet dsCOPTDTD004 = new DataSet();
                //DataSet dsINVMF = new DataSet();
                //int TempCount = 0;
                //int TempTotal = 0;
                //int sort = 0;
                //String pnum = null; 


                //sqlConn = new SqlConnection("server=192.168.2.27;database=tokichoi_new;uid=sa;pwd=dsc");

                
                ////找出未產生銷貨單的訂單
                //sbSql.Clear();
                //sbSql.Append(" SELECT TD001 ,TD002 FROM COPTD WITH (NOLOCK) ");
                //sbSql.Append(" WHERE NOT EXISTS (SELECT TH014 FROM COPTH WITH (NOLOCK) WHERE TH014=TD001 AND TH015=TD002 AND TH016=TD003) AND TD002>='201606010001'");
                //sbSql.Append(" GROUP BY TD001 ,TD002");

                //adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                //sqlCmdBuilder = new SqlCommandBuilder(adapter);

                //sqlConn.Open();
                //adapter.Fill(dsCOPTD, "TEMPCOPTD"); 
                //sqlConn.Close();

                //foreach (DataRow dr in dsCOPTD.Tables["TEMPCOPTD"].Rows) 
                //{
                //    //檢查庫存量是否足夠
                //    sbSql.Clear();
                //    sbSql.Append(" SELECT TD004,SUM(TD008) AS TD008  ");
                //    sbSql.Append("  ,ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0) AS TOTALINVMF");
                //    sbSql.Append("  ,ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) AS TOTALCOPTH");
                //    sbSql.Append("  ,(ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0)-SUM(TD008)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) ) AS CHECKTOTAL");
                //    sbSql.Append("  FROM COPTD WITH (NOLOCK)");
                //    sbSql.AppendFormat("  WHERE TD001='{0}' AND TD002='{1}'", dr["TD001"].ToString(), dr["TD002"].ToString());
                //    sbSql.Append("  GROUP BY TD004");
                //    sbSql.Append("  HAVING (ISNULL((SELECT SUM(MF008*MF010)  FROM INVMF WITH (NOLOCK) WHERE MF001=TD004 AND EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002)),0)-SUM(TD008)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=TD004 ),0) )<0");

                //    adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                //    sqlCmdBuilder = new SqlCommandBuilder(adapter);

                //    sqlConn.Open();
                //    adapter.Fill(dsCheckTotal, "TEMPdsCheckTotal");
                //    sqlConn.Close();

                //    //檢查庫存數量足夠，庫存不為負，才新增COPTG、COPTH
                //    if (dsCheckTotal.Tables["TEMPdsCheckTotal"].Rows.Count == 0)
                //    {
                //        //查出訂單中各品號及總數量
                //        sbSql.Clear();
                //        sbSql.Append(" SELECT TD001,TD002 ,TD004,CONVERT(int  ,SUM(TD008)) AS TD008  ");
                //        sbSql.Append(" FROM COPTD WITH (NOLOCK)    ");
                //        sbSql.AppendFormat("  WHERE TD001='{0}' AND TD002='{1}' GROUP BY TD001,TD002,TD004  ", dr["TD001"].ToString(), dr["TD002"].ToString());
                //        adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                //        sqlCmdBuilder = new SqlCommandBuilder(adapter);

                //        sqlConn.Open();
                //        adapter.Fill(dsCOPTDTD004, "TEMPdsCOPTDTD004");
                //        sqlConn.Close();

                //        sbSql.Clear();

                //        foreach (DataRow drdsCOPTDTD004 in dsCOPTDTD004.Tables["TEMPdsCOPTDTD004"].Rows)
                //        {
                //            TempTotal = Convert.ToInt16(drdsCOPTDTD004["TD008"].ToString());

                //            sbSql.Clear();
                //            sbSql.Append(" SELECT MF001,MF002,CONVERT(int,(SUM(MF008*MF010)-ISNULL((SELECT SUM(TH008)  FROM COPTH WITH (NOLOCK) WHERE TH020='N' AND  TH004=MF001 AND TH017=MF002 ),0)))  AS TOTAL");
                //            sbSql.Append(" FROM INVMF WITH (NOLOCK)");
                //            sbSql.AppendFormat(" WHERE EXISTS (SELECT ME006 FROM INVME WITH (NOLOCK) WHERE ME007='N' AND ME001=MF001 AND ME002=MF002) AND MF001='{0}'", drdsCOPTDTD004["TD004"].ToString());
                //            sbSql.Append(" GROUP BY  MF001,MF002");

                //            adapter = new SqlDataAdapter(@"" + sbSql, sqlConn);
                //            sqlCmdBuilder = new SqlCommandBuilder(adapter);
                //            adapter.Fill(dsINVMF, "TEMPdsINVMF");

                //            sbSql.Clear();
                          
                //            foreach (DataRow drTEMPdsINVMF in dsINVMF.Tables["TEMPdsINVMF"].Rows)
                //            {
                //                sort = sort + 1;
                //                pnum = sort.ToString().PadLeft(4, '0'); 

                //                TempTotal = TempTotal - Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString());
                //                //最後一批的批號滿足總數量
                //                if (TempTotal <= 0)
                //                {
                //                    sbSql.Append(" INSERT INTO tokichoi_new..COPTH(");
                //                    sbSql.Append(" COMPANY,CREATOR,USR_GROUP,CREATE_DATE,FLAG,CREATE_TIME,MODI_TIME,TRANS_TYPE,TRANS_NAME,TH001, TH002, TH003, TH004, TH005, TH006, TH007, TH008, TH009, TH010");
                //                    sbSql.Append(" ,TH011, TH012, TH013, TH014, TH015, TH016, TH017, TH018, TH019, TH020,TH021, TH022, TH023, TH024, TH025, TH026, TH027, TH028, TH029, TH030");
                //                    sbSql.Append(" ,TH031, TH032, TH033, TH034, TH035, TH036, TH037, TH038, TH039, TH040,TH041, TH042,  TH074, TH075, TH076, TH099");
                //                    sbSql.Append(" ) ");
                //                    sbSql.Append(" SELECT COPTD.COMPANY,COPTD.CREATOR,COPTD.USR_GROUP,COPTD.CREATE_DATE,COPTD.FLAG,COPTD.CREATE_TIME,COPTD.MODI_TIME,COPTD.TRANS_TYPE,COPTD.TRANS_NAME");
                //                    sbSql.AppendFormat(" ,TD001 AS TH001,TD002 AS TH002,'{0}' AS TH003,TD004 AS TH004,MB002 TH005,MB003 TH006,TD007 AS TH007,{1} AS TH008, MB004 AS TH009,0 AS TH010", pnum, TempTotal + Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString()));
                //                    sbSql.AppendFormat(" ,'' AS TH011,TD011 AS TH012,0 AS TH013,TD001 AS  TH014,TD002 AS  TH015, TD003 AS TH016,'{0}' AS TH017,'' AS TH018,'' AS TH019, 'N' AS TH020", drTEMPdsINVMF["MF002"].ToString());
                //                    sbSql.Append(" ,'N' AS TH021,'' AS  TH022,'' AS  TH023,'0' AS  TH024,'1' AS  TH025,'N' AS  TH026,'' AS  TH027,'' AS  TH028,'' AS  TH029,'' AS  TH030");
                //                    sbSql.Append(" ,'1' AS TH031,'' AS  TH032, '' AS TH033, '' AS TH034, 0 AS TH035,0 AS TH036,0 AS TH037,0 AS TH038,0 AS TH039,0 AS TH040");
                //                    sbSql.Append(" ,'' AS TH041,'N' AS TH042,'' AS  TH074,'' AS TH075,'' AS TH076,'' AS TH099");
                //                    sbSql.Append(" FROM INVMB WITH (NOLOCK)");
                //                    sbSql.AppendFormat(" LEFT JOIN COPTD WITH (NOLOCK) ON MB001=TD004 AND TD001='{0}' AND TD002='{1}'", drdsCOPTDTD004["TD001"].ToString(), drdsCOPTDTD004["TD002"].ToString());
                //                    sbSql.AppendFormat(" WHERE MB001='{0}' ", drTEMPdsINVMF["MF001"].ToString());
                //                    sbSql.Append(" ");
                //                    sbSql.Append(" ");
                //                    sbSql.Append(" ");

                //                    break;
                //                }
                //                else
                //                {
                //                    sbSql.Append(" INSERT INTO tokichoi_new..COPTH(");
                //                    sbSql.Append(" COMPANY,CREATOR,USR_GROUP,CREATE_DATE,FLAG,CREATE_TIME,MODI_TIME,TRANS_TYPE,TRANS_NAME,TH001, TH002, TH003, TH004, TH005, TH006, TH007, TH008, TH009, TH010");
                //                    sbSql.Append(" ,TH011, TH012, TH013, TH014, TH015, TH016, TH017, TH018, TH019, TH020,TH021, TH022, TH023, TH024, TH025, TH026, TH027, TH028, TH029, TH030");
                //                    sbSql.Append(" ,TH031, TH032, TH033, TH034, TH035, TH036, TH037, TH038, TH039, TH040,TH041, TH042,  TH074, TH075, TH076, TH099");
                //                    sbSql.Append(" ) ");
                //                    sbSql.Append(" SELECT COPTD.COMPANY,COPTD.CREATOR,COPTD.USR_GROUP,COPTD.CREATE_DATE,COPTD.FLAG,COPTD.CREATE_TIME,COPTD.MODI_TIME,COPTD.TRANS_TYPE,COPTD.TRANS_NAME");
                //                    sbSql.AppendFormat(" ,TD001 AS TH001,TD002 AS TH002,'{0}' AS TH003,TD004 AS TH004,MB002 TH005,MB003 TH006,TD007 AS TH007,{1} AS TH008, MB004 AS TH009,0 AS TH010", pnum, Convert.ToInt16(drTEMPdsINVMF["TOTAL"].ToString()));
                //                    sbSql.AppendFormat(" ,'' AS TH011,TD011 AS TH012,0 AS TH013,TD001 AS  TH014,TD002 AS  TH015, TD003 AS TH016,'{0}' AS TH017,'' AS TH018,'' AS TH019, 'N' AS TH020", drTEMPdsINVMF["MF002"].ToString());
                //                    sbSql.Append(" ,'N' AS TH021,'' AS  TH022,'' AS  TH023,'0' AS  TH024,'1' AS  TH025,'N' AS  TH026,'' AS  TH027,'' AS  TH028,'' AS  TH029,'' AS  TH030");
                //                    sbSql.Append(" ,'1' AS TH031,'' AS  TH032, '' AS TH033, '' AS TH034, 0 AS TH035,0 AS TH036,0 AS TH037,0 AS TH038,0 AS TH039,0 AS TH040");
                //                    sbSql.Append(" ,'' AS TH041,'N' AS TH042,'' AS  TH074,'' AS TH075,'' AS TH076,'' AS TH099");
                //                    sbSql.Append(" FROM INVMB WITH (NOLOCK)");
                //                    sbSql.AppendFormat(" LEFT JOIN COPTD WITH (NOLOCK) ON MB001=TD004 AND TD001='{0}' AND TD002='{1}'", drdsCOPTDTD004["TD001"].ToString(), drdsCOPTDTD004["TD002"].ToString());
                //                    sbSql.AppendFormat(" WHERE MB001='{0}' ", drTEMPdsINVMF["MF001"].ToString());
                //                    sbSql.Append(" ");
                //                    sbSql.Append(" ");
                //                    sbSql.Append(" ");
                //                }


                                
                //            }


                //        }

                //        sqlConn.Open();
                //        SqlTransaction tran = sqlConn.BeginTransaction();
                //        SqlCommand cmd = new SqlCommand();
                //        //新增COPTG
                //        //sbSql.Clear();
                //        sbSql.Append("  INSERT INTO COPTG ");
                //        sbSql.Append("  (COMPANY,CREATOR,USR_GROUP,CREATE_DATE,FLAG,CREATE_TIME,MODI_TIME,TRANS_TYPE,TRANS_NAME");
                //        sbSql.Append("  ,TG001, TG002, TG003, TG004, TG005, TG006, TG007, TG008, TG009, TG010,TG011, TG012, TG013, TG014, TG015, TG016, TG017, TG018, TG019, TG020");
                //        sbSql.Append("  ,TG021, TG022, TG023, TG024, TG025, TG026, TG027, TG028, TG029, TG030, TG031, TG032, TG033, TG034, TG035, TG036, TG037, TG038, TG039, TG040");
                //        sbSql.Append("  ,TG041, TG042, TG043, TG044, TG045, TG046, TG047, TG048, TG049, TG050,  TG051, TG052, TG053, TG054, TG055, TG056, TG057, TG058, TG059, TG060");
                //        sbSql.Append("  ,TG061, TG062, TG063, TG064, TG065, TG066, TG067, TG068, TG069, TG070,  TG071, TG072, TG073, TG074, TG075, TG076, TG080");
                //        sbSql.Append("  ,TG087, TG089, TG090, TG097, TG100,  TG106, TG107, TG108, TG109, TG110,  TG111, TG112, TG113, TG114, TG115,  TG116, TG118, TG120");
                //        sbSql.Append("  ,TG126, TG124, TG129,   TG130,  TG131, TG132, TG133,  TG134,  TG135, TG136,  TG137,  TG138, TG139,  TG140,  TG141");
                //        sbSql.Append("  )");
                //        sbSql.Append("  SELECT COPTC.COMPANY,COPTC.CREATOR,COPTC.USR_GROUP,CONVERT(varchar(8),GETDATE(), 112) AS CREATE_DATE,'1' AS FLAG,COPTC.CREATE_TIME,COPTC.MODI_TIME,'P003' AS TRANS_TYPE,'Copb09' AS TRANS_NAME");
                //        sbSql.Append("  ,TC001 AS TG001,TC002 AS TG002 ,CONVERT(varchar(8),GETDATE(), 112) AS TG003,TC004 AS TG004,TC005 AS TG005, TC006 AS TG006,TC053 AS TG007,TC010 AS TG008,TC011 AS TG009,TC007 AS TG010");
                //        sbSql.Append("  ,TC008 AS TG011,TC009 AS TG012,TC029 AS TG013,'' AS TG014,'' AS TG015,MA037 AS TG016,MA038 AS TG017,'' AS TG018,'' AS TG019, '' AS TG020");
                //        sbSql.Append("  ,CONVERT(varchar(8),GETDATE(), 112) AS  TG021,'0' AS TG022, 'N' AS TG023,'N' AS TG024,TC030 AS TG025,'' AS TG026,'' AS TG027,'' AS TG028,'' AS TG029, 'N' AS TG030");
                //        sbSql.Append("  ,'1' AS   TG031,TC031 AS  TG032,TC031 AS  TG033,'N' AS  TG034,'' AS  TG035,'N' AS  TG036,'N' AS  TG037,CONVERT(varchar(6),GETDATE(), 112) AS  TG038,'' AS  TG039,'' AS  TG040");
                //        sbSql.Append("  ,'0' AS   TG041,CONVERT(varchar(8),GETDATE(), 112)  AS  TG042,'' AS  TG043,TC041 AS  TG044, TC029 AS  TG045,TC030 AS  TG046,TC042 AS  TG047,TC001 AS  TG048,TC002 AS  TG049,'' AS  TG050");
                //        sbSql.Append("  ,'' AS   TG051,0 AS  TG052,0 AS  TG053,TC031 AS  TG054,'N' AS  TG055,'N' AS  TG056,'' AS  TG057,'' AS  TG058,'N' AS  TG059,'' AS  TG060");
                //        sbSql.Append("  ,'N' AS    TG061, 'N' AS  TG062,'0' AS   TG063,'' AS   TG064, '' AS  TG065,'' AS   TG066, '' AS  TG067,'1' AS   TG068, '0' AS  TG069, 'N' AS  TG070");
                //        sbSql.Append("  ,'0' AS   TG071,'4' AS  TG072,'' AS  TG073,'' AS  TG074, '' AS TG075,'' AS  TG076,'' AS  TG080");
                //        sbSql.Append("  , '' AS  TG087, 'N' AS TG089, 'N' AS TG090, 'N' AS TG097, 'N' AS TG100");
                //        sbSql.Append("  ,'' AS   TG106,MA004 AS  TG107,'' AS  TG108,'' AS  TG109,'' AS  TG110");
                //        sbSql.Append("  ,'1' AS   TG111,'' AS   TG112,'0' AS   TG113,'0' AS   TG114,'N' AS   TG115, 'N' AS   TG116,'' AS   TG118,'' AS   TG120");
                //        sbSql.Append("  , '' AS  TG126, '' AS  TG124,'' AS   TG129, '' AS   TG130");
                //        sbSql.Append("  ,'' AS   TG131, '0' AS  TG132,'' AS   TG133, '' AS  TG134,'1' AS   TG135,'0' AS   TG136,'0' AS   TG137,'0' AS   TG138,'0' AS   TG139,'0' AS   TG140,'0' AS   TG141");
                //        sbSql.Append("  FROM COPTC WITH (NOLOCK),COPMA  WITH (NOLOCK)");
                //        sbSql.AppendFormat("  WHERE TC004=MA001 AND TC001='{0}' AND TC002='{1}'", dr["TD001"].ToString(), dr["TD002"].ToString());
                //        sbSql.Append("  ");


                //        cmd.Connection = sqlConn;
                //        cmd.CommandTimeout = 60;
                //        cmd.CommandText = sbSql.ToString();
                //        cmd.Transaction = tran;
                //        int result = cmd.ExecuteNonQuery();

                //        if (result == 0)
                //        {
                //            tran.Rollback();    //交易取消
                //        }
                //        else
                //        {
                //            tran.Commit();      //執行交易
                //        }
                //        sqlConn.Close();
                //    }
                

                //}

                #endregion 


            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }       
    }


}
