using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Diagnostics;
using TeamDev.Redis.LanguageItems;
using TeamDev.Redis.Interface;
using System.Threading;
using System.Net.Mail;

namespace TeamDev.Redis
{
  public class RedisDataAccessProvider : DataAccessProvider, IDisposable
  {
    private static StringBuilder errorlog = new StringBuilder();

    #region private fields
    //private Socket _socket;

    private volatile ReaderWriterLock _socketslock = new ReaderWriterLock();
    private volatile Dictionary<int, Socket> _sockets = new Dictionary<int, Socket>();
    private volatile Dictionary<int, BufferedStream> _bstreams = new Dictionary<int, BufferedStream>();
    private volatile byte[] _end_data = new byte[] { (byte)'\r', (byte)'\n' };
    private volatile CommandTracing _tracer = new CommandTracing();

    #endregion

    public LanguageItemCollection<LanguageList> List { get; private set; }
    public LanguageItemCollection<LanguageSet> Set { get; private set; }
    public LanguageItemCollection<LanguageSortedSet> SortedSet { get; private set; }
    public LanguageItemCollection<LanguageHash> Hash { get; private set; }
    public LanguageItemCollection<LanguageString> Strings { get; private set; }
    public LanguageKey Key { get; private set; }
    public LanguageTransactions Transaction { get; private set; }
    public LanguageMessaging Messaging { get; private set; }

    #region constructor

    public RedisDataAccessProvider()
      : base()
    {
      base.Configuration.Host = "localhost";
      base.Configuration.Port = 6379;
      base.Configuration.ReceiveTimeout = -1;

      List = new LanguageItemCollection<LanguageList>() { Provider = this };
      Set = new LanguageItemCollection<LanguageSet>() { Provider = this };
      SortedSet = new LanguageItemCollection<LanguageSortedSet>() { Provider = this };
      Hash = new LanguageItemCollection<LanguageHash>() { Provider = this };
      this.Strings = new LanguageItemCollection<LanguageString>() { Provider = this };
      Key = new LanguageKey();
      Transaction = new LanguageTransactions();
      Messaging = new LanguageMessaging();

      ((ILanguageItem)Key).Configure(string.Empty, this);
      ((ILanguageItem)Transaction).Configure(string.Empty, this);
      ((ILanguageItem)Messaging).Configure(string.Empty, this);
    }

    #endregion

    #region destructor

    ~RedisDataAccessProvider()
    {
      Dispose(false);
    }

    #endregion

    #region IDisposable

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
      if (disposing)
      {
        Close();
      }
    }

    #endregion

    #region connection methods
    public override Socket Connect()
    {
      var tid = Thread.CurrentThread.ManagedThreadId;

      _socketslock.AcquireReaderLock(1000);
      if (_sockets.ContainsKey(tid))
      {
        var result = _sockets[tid];
        _socketslock.ReleaseReaderLock();
        return result;
      }

      var newsocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      newsocket.NoDelay = true;
      if (Configuration.SendTimeout > 0)
        newsocket.SendTimeout = Configuration.SendTimeout;

      if (Configuration.ReceiveTimeout > 0)
        newsocket.ReceiveTimeout = Configuration.ReceiveTimeout;

      newsocket.Connect(Configuration.Host, Configuration.Port);
      if (!newsocket.Connected)
      {
        newsocket.Close();
        return null;
      }

      _socketslock.UpgradeToWriterLock(1000);
      _sockets.Add(tid, newsocket);
      if (_bstreams.ContainsKey(tid))
        _bstreams[tid] = new BufferedStream(new NetworkStream(newsocket), 16 * 1024);
      else
        _bstreams.Add(tid, new BufferedStream(new NetworkStream(newsocket), 16 * 1024));
      _socketslock.ReleaseLock();

      if (Configuration.Password != null)
      {
        SendCommand(RedisCommand.AUTH, Configuration.Password);
        WaitComplete();
      }

      return newsocket;
    }

    private void Quit()
    {
      if (GetSocket() != null) SendCommand(RedisCommand.QUIT);
    }

    private Socket GetSocket()
    {
      var tid = Thread.CurrentThread.ManagedThreadId;

      try
      {
        _socketslock.AcquireReaderLock(1000);
        if (_sockets.ContainsKey(tid))
          return _sockets[tid];
        return null;
      }
      finally
      {
        _socketslock.ReleaseLock();
      }
    }

    private BufferedStream GetBStream()
    {
      var tid = Thread.CurrentThread.ManagedThreadId;
      try
      {
        _socketslock.AcquireReaderLock(1000);
        var result = _bstreams[tid];
        return result;
      }
      finally
      {
        _socketslock.ReleaseLock();
      }
    }

    private void RemoveSocket()
    {
      var tid = Thread.CurrentThread.ManagedThreadId;

      _socketslock.AcquireReaderLock(1000);
      if (_sockets.ContainsKey(tid))
      {
        _socketslock.UpgradeToWriterLock(1000);
        _sockets.Remove(tid);
      }
      _socketslock.ReleaseLock();
    }

    public override void Close()
    {
      Quit();
      GetSocket().Close();
      RemoveSocket();
    }

    #endregion

    #region communication methods

    public int SendCommand(RedisCommand command, params string[] args)
    {
      this.Connect();

      // http://redis.io/topics/protocol
      // new redis communication protocol specifications

      StringBuilder sb = new StringBuilder();
      sb.AppendFormat("*{0}\r\n", args != null ? args.Length + 1 : 1);

      var cmd = command.ToString();
      sb.AppendFormat("${0}\r\n{1}\r\n", cmd.Length, cmd);

      if (args != null)
        foreach (var arg in args)
        {
          sb.AppendFormat("${0}\r\n{1}\r\n", arg.Length, arg);
        }

      byte[] r = Encoding.UTF8.GetBytes(sb.ToString());
      try
      {
        Log("S: " + String.Format(cmd, args));
        GetSocket().Send(r);
      }
      catch (SocketException)
      {
        // timeout;
        GetSocket().Close();
        RemoveSocket();
        return 0;
      }
      if (Configuration.LogUnbalancedCommands)
        return _tracer.TraceCommand(command);
      return 0;
    }

    public int SendCommand(RedisCommand command, byte[] datas, params string[] args)
    {
      this.Connect();

      // http://redis.io/topics/protocol
      // new redis communication protocol specifications

      StringBuilder sb = new StringBuilder();

      int acount = args != null ? args.Length + 1 : 1;
      acount += datas != null && datas.Length > 0 ? 1 : 0;

      sb.AppendFormat("*{0}\r\n", acount);

      var cmd = command.ToString();
      sb.AppendFormat("${0}\r\n{1}\r\n", cmd.Length, cmd);

      if (args != null)
        foreach (var arg in args)
        {
          sb.AppendFormat("${0}\r\n{1}\r\n", arg.Length, arg);
        }

      byte[] r = Encoding.UTF8.GetBytes(sb.ToString());
      var socket = GetSocket();

      try
      {
        Log("S: " + String.Format(cmd, args));
        // Send command and args. 
        socket.Send(r);

        // Send data
        if (datas != null && datas.Length > 0)
        {
          socket.Send(datas);
          socket.Send(Encoding.UTF8.GetBytes("\r\n"));
        }

      }
      catch (SocketException)
      {
        // timeout;
        socket.Close();
        RemoveSocket();
        return 0;
      }
      if (Configuration.LogUnbalancedCommands)
        return _tracer.TraceCommand(command);
      return 0;
    }

    public int SendCommand(RedisCommand command, IDictionary<string, byte[]> datas, params string[] args)
    {
      this.Connect();

      // http://redis.io/topics/protocol
      // new redis communication protocol specifications

      StringBuilder sb = new StringBuilder();

      int acount = args != null ? args.Length + 1 : 1;
      acount += datas != null && datas.Count > 0 ? datas.Count * 2 : 0;

      sb.AppendFormat("*{0}\r\n", acount);

      var cmd = command.ToString();
      sb.AppendFormat("${0}\r\n{1}\r\n", cmd.Length, cmd);

      if (args != null)
        foreach (var arg in args)
        {
          sb.AppendFormat("${0}\r\n{1}\r\n", arg.Length, arg);
        }

      MemoryStream ms = new MemoryStream();

      byte[] r = Encoding.UTF8.GetBytes(sb.ToString());
      ms.Write(r, 0, r.Length);

      if (datas != null && datas.Count > 0)
      {
        foreach (var data in datas)
        {
          r = Encoding.UTF8.GetBytes(string.Format("${0}\r\n{1}\r\n", data.Key.Length, data.Key));
          ms.Write(r, 0, r.Length);
          r = Encoding.UTF8.GetBytes(string.Format("${0}\r\n", data.Value.Length));
          ms.Write(r, 0, r.Length);
          ms.Write(data.Value, 0, data.Value.Length);
          ms.Write(_end_data, 0, 2);
        }
      }

      var socket = GetSocket();
      try
      {
        Log("S: " + String.Format(cmd, args));
        socket.Send(ms.ToArray());
      }
      catch (SocketException)
      {
        // timeout;
        socket.Close();
        RemoveSocket();
        return 0;
      }
      if (Configuration.LogUnbalancedCommands)
        return _tracer.TraceCommand(command);
      return 0;
    }

    public int SendCommand(RedisCommand command, IDictionary<string, string> datas, params string[] args)
    {
      var result = new Dictionary<string, byte[]>();

      foreach (var kv in datas)
        result.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));

      return SendCommand(command, result, args);
    }

    public void WaitComplete(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      int c = GetBStream().ReadByte();
      if (c == -1)
        throw new Exception("No more data");

      var s = ReadLine();
      Log((char)c + s);
      if (c == '-')
        throw new Exception(s.StartsWith("ERR") ? s.Substring(4) : s);
    }

    public int ReadInt(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      var s = ReadLine();
      if (string.IsNullOrEmpty(s))
        s = ReadLine();

      if (string.IsNullOrEmpty(s))
      {
        SendExceptionLog();
        throw new InvalidDataException("Unexpected value reading data from TCP/IP channel ");
      }

      var c = s[0];
      if (s[0] == ':')
      {
        int n = 0;
        if (int.TryParse(s.Substring(1), out n))
          return n;
      }
      Log((char)c + s);
      if (c == '-')
        throw new Exception(s.StartsWith("ERR") ? s.Substring(4) : s);

      throw new Exception("Unexpected response ");
    }

    public byte[] ReadData(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      string r = ReadLine();
      Log("R: {0}", r);
      if (r.Length == 0)
        throw new Exception("Zero length respose");

      char c = r[0];
      if (c == '-')
        throw new Exception(r.StartsWith("-ERR") ? r.Substring(5) : r.Substring(1));

      if (c == '$')
      {
        if (r == "$-1")
          return null;
        int n;

        var bstream = GetBStream();
        if (Int32.TryParse(r.Substring(1), out n))
        {
          byte[] retbuf = new byte[n];
          if (n > 0)
          {

            int bytesRead = 0;
            do
            {
              int read = bstream.Read(retbuf, bytesRead, n - bytesRead);
              if (read < 1)
                throw new Exception("Invalid termination mid stream");
              bytesRead += read;
            }
            while (bytesRead < n);
          }
          if (bstream.ReadByte() != '\r' || bstream.ReadByte() != '\n')
            throw new Exception("Invalid termination");
          Log("R: {0}, {1}", r, Encoding.UTF8.GetString(retbuf));

          return retbuf;
        }
        throw new Exception("Invalid length");
      }

      //returns the number of matches
      if (c == '*')
      {
        int n;
        if (Int32.TryParse(r.Substring(1), out n))
          return n <= 0 ? new byte[0] : ReadData();

        throw new Exception("Unexpected length parameter" + r);
      }

      throw new Exception("Unexpected reply: " + r);
    }

    public byte[][] ReadMulti(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      string r = ReadLine();
      Log("R: {0}", r);
      if (r.Length == 0)
        throw new Exception("Zero length respose");

      char c = r[0];
      if (c == '-')
        throw new Exception(r.StartsWith("-ERR") ? r.Substring(5) : r.Substring(1));

      List<byte[]> result = new List<byte[]>();

      if (c == '*')
      {
        int n;
        if (Int32.TryParse(r.Substring(1), out n))
          for (int i = 0; i < n; i++)
          {
            result.Add(ReadData());
          }
      }
      return result.ToArray();
    }

    public string[] ReadMultiString(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      string r = ReadLine();
      Log("R: {0}", r);
      if (r.Length == 0)
        throw new Exception("Zero length respose");

      char c = r[0];
      if (c == '-')
        throw new Exception(r.StartsWith("-ERR") ? r.Substring(5) : r.Substring(1));

      List<string> result = new List<string>();

      if (c == '*')
      {
        int n;
        if (Int32.TryParse(r.Substring(1), out n))
          for (int i = 0; i < n; i++)
          {
            result.Add(ReadString());
          }
      }
      return result.ToArray();
    }

    public string ReadString(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      return Encoding.UTF8.GetString(ReadData());
    }

    private string ReadLine(int commandid = 0)
    {
      if (commandid != 0 && Configuration.LogUnbalancedCommands)
        _tracer.CheckBalancing(commandid);

      var sb = new StringBuilder();
      int c;
      var bstream = GetBStream();
      while ((c = bstream.ReadByte()) != -1)
      {
        if (c == '\r')
          continue;
        if (c == '\n')
          break;
        sb.Append((char)c);
      }
      return sb.ToString();
    }

    [Conditional("DEBUG")]
    private void Log(string fmt, params object[] args)
    {
      Debug.WriteLine("{0}", String.Format(fmt, args).Trim());
      errorlog.AppendFormat("{0}\r\n", string.Format(fmt, args).Trim());
    }

    private static void SendExceptionLog()
    {
      SmtpClient client = new SmtpClient("uhura.teamdev.it");
      MailMessage mm = new MailMessage();
      mm.To.Add("paolo@teamdev.it");
      mm.Subject = "Redis Client AutomatedException";
      mm.From = new MailAddress("redis@teamdev.it");
      mm.Body = errorlog.ToString();
      mm.IsBodyHtml = false;

      client.Send(mm);
    }

    #endregion

  }
}
