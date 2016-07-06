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
    public partial class Form1 : Form
    {
        //設定參數

        //form區
        #region
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            timer1.Interval = 1000; // 設定每秒觸發一次
            //timer1.Interval = 1000 * 60; // 設定每分觸發一次
            timer1.Enabled = true; // 啟動 Timer

        }

        #endregion

        //排程執行區-timer1_Tick 判斷
        #region
        private void timer1_Tick(object sender, EventArgs e)
        {
            if(button1.Text.ToString().Equals("關閉"))
            {   
                if (radioButton1.Checked == true)
                {
                    richTextBox1.Text = "週期-下次執行時間 " + Environment.NewLine + DateTime.Now.ToString();
                    CreateCOP COP = new CreateCOP();
                    COP.CreateCOPGH();
                    //ExeSQL1();
                }
                else if (radioButton2.Checked == true)
                {
                    richTextBox1.Text = "每天-下次執行時間 " + Environment.NewLine + dateTimePicker1.Value.ToShortTimeString();
                    if (dateTimePicker1.Value.ToShortTimeString().Equals(DateTime.Now.ToShortTimeString()))
                    {
                        //ExeSQL1();
                    }
                }
                else if (radioButton3.Checked == true)
                {
                    richTextBox1.Text = "固定某日+時間-下次執行時間 " + Environment.NewLine + "日期 " + numericUpDown2.Value.ToString() + " 時間 " + dateTimePicker1.Value.ToShortTimeString();
                    if ((dateTimePicker1.Value.ToShortTimeString().Equals(DateTime.Now.ToShortTimeString())) && (DateTime.Now.DayOfWeek.ToString("d").Equals(numericUpDown2.Value.ToString())))
                    {
                        //ExeSQL1();
                    }
                }
                else if (radioButton4.Checked == true)
                {
                    richTextBox1.Text = "固定星期+時間-下次執行時間 " + Environment.NewLine + "星期 " + numericUpDown3.Value.ToString() + " 時間 " + dateTimePicker1.Value.ToShortTimeString();
                    if ((dateTimePicker1.Value.ToShortTimeString().Equals(DateTime.Now.ToShortTimeString())) && (DateTime.Now.DayOfWeek.ToString("d").Equals(numericUpDown3.Value.ToString())))
                    {
                        //ExeSQL1();
                    }
                }
                else
                {
                    //count = 0;
                    //textBox1.Text = textBox1.Text + count.ToString() + " ExeSQL1 WAIT!! " + Environment.NewLine; 
                }

            }
        }
        #endregion

        //排程程式
        #region

        #endregion

        //按鈕功能區
        #region
        private void button1_Click(object sender, EventArgs e)
        {
            if(button1.Text.ToString().Equals("啟動"))
            {
                button1.Text = "關閉";
                button1.BackColor = Color.Red;
                tableLayoutPanel4.BackColor = Color.Green;
                label13.Text = "RUNNIING";
                //label13.ForeColor = Color.Green;
            }
            else
            {
                button1.Text = "啟動";
                button1.BackColor = Color.Green;
                tableLayoutPanel4.BackColor = Color.Red;
                label13.Text = "STOP";
                //label13.ForeColor = Color.Red;
            }
        }

        #endregion


       

    }
}
