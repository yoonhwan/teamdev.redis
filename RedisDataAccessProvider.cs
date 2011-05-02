using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Diagnostics;
using TeamDev.Redis.LanguageItems;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis
{
  public class RedisDataAccessProvider : DataAccessProvider, IDisposable
  {
    #region private fields
    private Socket _socket;
    private BufferedStream _bstream;
    private byte[] _end_data = new byte[] { (byte)'\r', (byte)'\n' };
    #endregion

    public LanguageItemCollection<LanguageList> List { get; private set; }
    public LanguageItemCollection<LanguageSet> Set { get; private set; }
    public LanguageItemCollection<LanguageSortedSet> SortedSet { get; private set; }
    public LanguageItemCollection<LanguageHash> Hash { get; private set; }
    public LanguageKey Key { get; private set; }

    #region constructor

    public RedisDataAccessProvider()
      : base()
    {
      base.Configuration.Host = "localhost";
      base.Configuration.Port = 6379;

      List = new LanguageItemCollection<LanguageList>() { Provider = this };
      Set = new LanguageItemCollection<LanguageSet>() { Provider = this };
      SortedSet = new LanguageItemCollection<LanguageSortedSet>() { Provider = this };
      Hash = new LanguageItemCollection<LanguageHash>() { Provider = this };
      Key = new LanguageKey();
      ((ILanguageItem)Key).Configure(string.Empty, this);
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
    public override void Connect()
    {
      // I already have a socket.
      if (_socket != null) return;

      _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      _socket.NoDelay = true;
      _socket.SendTimeout = Configuration.SendTimeout;
      _socket.Connect(Configuration.Host, Configuration.Port);
      if (!_socket.Connected)
      {
        _socket.Close();
        _socket = null;
        return;
      }
      _bstream = new BufferedStream(new NetworkStream(_socket), 16 * 1024);

      if (Configuration.Password != null)
      {
        SendCommand(RedisCommand.AUTH, Configuration.Password);
        WaitComplete();
      }
    }

    private void Quit()
    {
      if (_socket != null) SendCommand(RedisCommand.QUIT);
    }

    public override void Close()
    {
      Quit();
      _socket.Close();
      _socket = null;
    }

    #endregion

    #region communication methods

    public bool SendCommand(RedisCommand command, params string[] args)
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
        _socket.Send(r);
      }
      catch (SocketException)
      {
        // timeout;
        _socket.Close();
        _socket = null;

        return false;
      }
      return true;
    }

    public bool SendCommand(RedisCommand command, byte[] datas, params string[] args)
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
      try
      {
        Log("S: " + String.Format(cmd, args));
        // Send command and args. 
        _socket.Send(r);

        // Send data
        if (datas != null && datas.Length > 0)
        {
          _socket.Send(datas);
          _socket.Send(Encoding.UTF8.GetBytes("\r\n"));
        }

      }
      catch (SocketException)
      {
        // timeout;
        _socket.Close();
        _socket = null;

        return false;
      }
      return true;
    }

    public bool SendCommand(RedisCommand command, IDictionary<string, byte[]> datas, params string[] args)
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

      try
      {
        Log("S: " + String.Format(cmd, args));
        _socket.Send(ms.ToArray());
      }
      catch (SocketException)
      {
        // timeout;
        _socket.Close();
        _socket = null;

        return false;
      }
      return true;
    }

    public bool SendCommand(RedisCommand command, IDictionary<string, string> datas, params string[] args)
    {
      var result = new Dictionary<string, byte[]>();

      foreach (var kv in datas)
        result.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));

      return SendCommand(command, result, args);
    }

    public void WaitComplete()
    {
      int c = _bstream.ReadByte();
      if (c == -1)
        throw new Exception("No more data");

      var s = ReadLine();
      Log((char)c + s);
      if (c == '-')
        throw new Exception(s.StartsWith("ERR") ? s.Substring(4) : s);
    }

    public int ReadInt()
    {
      int c = _bstream.ReadByte();
      if (c == -1)
        throw new Exception("No more data");

      var s = ReadLine();
      int n = 0;
      if (int.TryParse(s, out n))
        return n;

      Log((char)c + s);
      if (c == '-')
        throw new Exception(s.StartsWith("ERR") ? s.Substring(4) : s);

      throw new Exception("Unexpected response ");
    }

    public byte[] ReadData()
    {
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

        if (Int32.TryParse(r.Substring(1), out n))
        {
          byte[] retbuf = new byte[n];

          int bytesRead = 0;
          do
          {
            int read = _bstream.Read(retbuf, bytesRead, n - bytesRead);
            if (read < 1)
              throw new Exception("Invalid termination mid stream");
            bytesRead += read;
          }
          while (bytesRead < n);
          if (_bstream.ReadByte() != '\r' || _bstream.ReadByte() != '\n')
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

    public byte[][] ReadMulti()
    {
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

    public string[] ReadMultiString()
    {
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

    public string ReadString()
    {
      return Encoding.UTF8.GetString(ReadData());
    }

    private string ReadLine()
    {
      var sb = new StringBuilder();
      int c;

      while ((c = _bstream.ReadByte()) != -1)
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
      Console.WriteLine("{0}", String.Format(fmt, args).Trim());
    }

    #endregion


  }
}
