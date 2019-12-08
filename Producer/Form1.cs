using Newtonsoft.Json;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Producer
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }       
        
        public class FileChunk
        {
            public string Hash { get; set; }
            public byte[] Content { get; set; }
            public int ChunkN { get; set; }
            public string FileName { get; set; }
            public int ChunkCount { get; set; }
        }

        public string path;
        public List<string> paths= new List<string>();
        public string hash;

        private void button1_Click(object sender, EventArgs e)
        {            
            if (openFileDialog1.ShowDialog() == DialogResult.Cancel)
                return;
            if (!checkBox1.Checked)
            {
                path = openFileDialog1.FileName;
                ReadingFile(path);
            }                
            else
            {
                paths.AddRange(openFileDialog1.FileNames);
                foreach (var item in paths)
                {
                    ReadingFile(item);
                }
            }                    
        }

        public void ReadingFile(string path)
        {
            FileInfo fi = new FileInfo(path);

            int countP;            
            if((fi.Length % 1048576) != 0)
                countP = Convert.ToInt32(fi.Length / 1048576)+1;
            else
                countP = Convert.ToInt32(fi.Length / 1048576);            
            
            string hash = ComputeMD5Checksum(path);            

            var allBytes = File.ReadAllBytes(path);

            int part = 1;
            int partsize = 1024 * 1024;
            int position = 0;

            for (int i = 0; i < allBytes.Length; i += partsize)
            {
                byte[] partbytes = new byte[Math.Min(partsize, allBytes.Length - i)];
                for (int j = 0; j < partbytes.Length; j++)
                {
                    partbytes[j] = allBytes[position++];
                }
                FileChunk fc = new FileChunk()
                {
                    Hash = hash,
                    Content = partbytes,
                    ChunkN = part,
                    FileName = fi.Name,
                    ChunkCount = countP
                };
                SendF(fc);
                part++;
            }
            
        }

        public void SendF(FileChunk fileChunk)
        {
            var factory = new ConnectionFactory() { HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "files",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var jsonObj = JsonConvert.SerializeObject(fileChunk);
                var body = Encoding.UTF8.GetBytes(jsonObj);                

                channel.BasicPublish(exchange: "",
                                     routingKey: "files",
                                     basicProperties: null,
                                     body: body);
            }
        }
        
        private string ComputeMD5Checksum(string path)
        {
            using (FileStream fs = System.IO.File.OpenRead(path))
            {
                MD5 md5 = new MD5CryptoServiceProvider();
                byte[] fileData = new byte[fs.Length];
                fs.Read(fileData, 0, (int)fs.Length);
                byte[] checkSum = md5.ComputeHash(fileData);
                string result = BitConverter.ToString(checkSum).Replace("-", String.Empty);
                return result;
            }
        }

        private void checkBox1_CheckedChanged(object sender, EventArgs e)
        {            
            if (checkBox1.Checked)
                openFileDialog1.Multiselect = true;
            else
                openFileDialog1.Multiselect = false;
        }
    }
}

