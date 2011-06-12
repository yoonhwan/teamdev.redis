using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.ComponentModel;
using TeamDev.Redis.Interface;

namespace TeamDev.Redis.LanguageItems
{
  public class LanguageKey : ILanguageItem
  {
    internal string _name;
    internal RedisDataAccessProvider _provider;

    public bool Remove(params string[] keys)
    {      
      return _provider.ReadInt(_provider.SendCommand(RedisCommand.DEL, keys)) == 1;
    }

    public bool Exists(string key)
    {      
      return _provider.ReadInt(_provider.SendCommand(RedisCommand.EXISTS, key)) == 1;
    }

    [Description("Return all keys matching the pattern")]
    public string[] this[string pattern]
    {
      get
      {        
        return _provider.ReadMultiString(_provider.SendCommand(RedisCommand.KEYS, pattern));
      }
    }

    public string Type(string key)
    {      
      return _provider.ReadString(_provider.SendCommand(RedisCommand.TYPE, key));
    }

    void ILanguageItem.Configure(string name, RedisDataAccessProvider provider)
    {
      _name = name;
      _provider = provider;
    }
  }
}
