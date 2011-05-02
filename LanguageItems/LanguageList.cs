using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageList : ILanguageItem, IComplexItem
  {
    internal string _name = string.Empty;
    internal RedisDataAccessProvider _provider = null;

    [Description("Append a value to the list")]
    public void Append(string value)
    {
      _provider.InternalSendCommand(RedisCommand.RPUSH, _name, value);
      _provider.WaitComplete();
    }

    [Description("Prepend a value to the list")]
    public void Prepend(string value)
    {
      _provider.InternalSendCommand(RedisCommand.LPUSH, _name, value);
      _provider.WaitComplete();
    }

    [Description("Remove all elements with the given value from the list. ")]
    public void Remove(string value)
    {
      _provider.InternalSendCommand(RedisCommand.LREM, _name, "0", value);
      _provider.WaitComplete();
    }

    [Description("Clear the list")]
    public void Clear()
    {
      _provider.InternalSendCommand(RedisCommand.DEL, _name);
      _provider.WaitComplete();
    }

    /// <summary>
    /// LINDEX key index - 
    /// Get an element from a list by its index
    /// 
    /// LSET key index value -
    /// Set the value of an element in a list by its index
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    [Description("Get or set an element from a list by its index")]
    public string this[int index]
    {
      get
      {
        _provider.InternalSendCommand(RedisCommand.LINDEX, _name, index.ToString());
        return Encoding.UTF8.GetString(_provider.ReadData());
      }
      set
      {
        _provider.InternalSendCommand(RedisCommand.LSET, _name, index.ToString(), value);
        _provider.WaitComplete();
      }
    }

    public string LeftPop()
    {
      _provider.InternalSendCommand(RedisCommand.LPOP, _name);
      return _provider.ReadString();
    }

    public string RightPop()
    {
      _provider.InternalSendCommand(RedisCommand.RPOP, _name);
      return _provider.ReadString();
    }

    /// <summary>
    /// LLEN key
    /// Get the length of a list
    /// </summary>
    public int Count
    {
      get
      {
        _provider.InternalSendCommand(RedisCommand.LLEN, _name);
        return _provider.ReadInt();
      }
    }

    public string[] Range(int startindex, int endindex)
    {
      _provider.InternalSendCommand(RedisCommand.LRANGE, _name, startindex.ToString(), endindex.ToString());
      _provider.ReadData();
      return _provider.ReadMultiString();
    }

    public string[] Values
    {
      get
      {
        _provider.InternalSendCommand(RedisCommand.LRANGE, _name, "0", "-1");
        return _provider.ReadMultiString();
      }
    }

    void ILanguageItem.Configure(string name, RedisDataAccessProvider provider)
    {
      _name = name;
      _provider = provider;
    }

    string IComplexItem.KeyName
    {
      get { return _name; }
    }

    RedisDataAccessProvider IComplexItem.Provider
    {
      get { return _provider; }
    }

  }
}
