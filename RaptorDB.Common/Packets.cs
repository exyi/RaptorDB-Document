using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB.Common
{
    public class Packet
    {
        public Packet()
        {
            OrderBy = "";
        }
        public string Username { get; set; }
        public string PasswordHash { get; set; }
        //public int Token { get; set; }
        //public int Session { get; set; }
        public PacketCommand Command { get; set; }
        public object Data { get; set; }
        public Guid Docid { get; set; }
        public string Viewname { get; set; }
        public int Start { get; set; }
        public int Count { get; set; }
        public string OrderBy { get; set; }
    }

    public class ReturnPacket
    {
        public ReturnPacket()
        {

        }
        public ReturnPacket(bool ok)
        {
            OK = ok;
        }
        public ReturnPacket(bool ok, string err)
        {
            OK = ok;
            Error = err;
        }
        public string Error { get; set; }
        public bool OK { get; set; }
        //public int Token { get; set; }
        //public int Session { get; set; }
        public object Data { get; set; }
    }

    public enum PacketCommand: int
    {
        Save,
        SaveBytes,
        QueryType,
        QueryStr,
        Fetch,
        FetchBytes,
        Backup,
        Delete,
        DeleteBytes,
        Restore,
        AddUser,
        ServerSide,
        FullText,
        CountType,
        CountStr,
        GCount,
        DocHistory,
        FileHistory,
        FetchVersion,
        FetchFileVersion,
        CheckAssembly,
        FetchHistoryInfo,
        FetchByteHistoryInfo,
        ViewDelete,
        ViewDelete_t,
        ViewInsert,
        ViewInsert_t,
        DocCount,
        GetObjectHF,
        SetObjectHF,
        DeleteKeyHF,
        CountHF,
        ContainsHF,
        GetKeysHF,
        CompactStorageHF
    }
}
