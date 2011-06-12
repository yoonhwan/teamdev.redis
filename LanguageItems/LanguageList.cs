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
      _provider.ReadInt(_provider.SendCommand(RedisCommand.RPUSH, _name, value));
    }

    [Description("Prepend a value to the list")]
    public void Prepend(string value)
    {
      _provider.ReadInt(_provider.SendCommand(RedisCommand.LPUSH, _name, value));
    }

    [Description("Remove all elements with the given value from the list. ")]
    public void Remove(string value)
    {
      _provider.ReadInt(_provider.SendCommand(RedisCommand.LREM, _name, "0", value));
    }

    [Description("Clear the list")]
    public void Clear()
    {
      _provider.WaitComplete(_provider.SendCommand(RedisCommand.DEL, _name));
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
        return Encoding.UTF8.GetString(_provider.ReadData(_provider.SendCommand(RedisCommand.LINDEX, _name, index.ToString())));
      }
      set
      {
        _provider.WaitComplete(_provider.SendCommand(RedisCommand.LSET, _name, index.ToString(), value));
      }
    }

    public string LeftPop()
    {
      return _provider.ReadString(_provider.SendCommand(RedisCommand.LPOP, _name));
    }

    public string RightPop()
    {
      return _provider.ReadString(_provider.SendCommand(RedisCommand.RPOP, _name));
    }

    /// <summary>
    /// LLEN key
    /// Get the length of a list
    /// </summary>
    public int Count
    {
      get
      {
        return _provider.ReadInt(_provider.SendCommand(RedisCommand.LLEN, _name));
      }
    }

    public string[] Range(int startindex, int endindex)
    {
      return _provider.ReadMultiString(_provider.SendCommand(RedisCommand.LRANGE, _name, startindex.ToString(), endindex.ToString()));
    }

    public string[] Values
    {
      get
      {
        return _provider.ReadMultiString(_provider.SendCommand(RedisCommand.LRANGE, _name, "0", "-1"));
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
